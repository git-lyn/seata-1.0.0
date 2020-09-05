/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.core.store.db;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import io.seata.common.exception.DataAccessException;
import io.seata.common.exception.StoreException;
import io.seata.common.executor.Initialize;
import io.seata.common.loader.LoadLevel;
import io.seata.common.util.CollectionUtils;
import io.seata.common.util.IOUtil;
import io.seata.common.util.LambdaUtils;
import io.seata.common.util.StringUtils;
import io.seata.config.Configuration;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.constants.ServerTableColumnsName;
import io.seata.core.store.LockDO;
import io.seata.core.store.LockStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Data base lock store.
 *
 * @author zhangsen
 */
@LoadLevel(name = "db")
public class LockStoreDataBaseDAO implements LockStore, Initialize {

    private static final Logger LOGGER = LoggerFactory.getLogger(LockStoreDataBaseDAO.class);

    /**
     * The constant CONFIG.
     */
    protected static final Configuration CONFIG = ConfigurationFactory.getInstance();

    /**
     * The Log store data source.
     */
    protected DataSource logStoreDataSource = null;

    /**
     * The Lock table.
     */
    protected String lockTable;

    /**
     * The Db type.
     */
    protected String dbType;

    /**
     * Instantiates a new Data base lock store dao.
     *
     * @param logStoreDataSource the log store data source
     */
    public LockStoreDataBaseDAO(DataSource logStoreDataSource) {
        this.logStoreDataSource = logStoreDataSource;
    }

    @Override
    public void init() {
        lockTable = CONFIG.getConfig(ConfigurationKeys.LOCK_DB_TABLE, ConfigurationKeys.LOCK_DB_DEFAULT_TABLE);
        dbType = CONFIG.getConfig(ConfigurationKeys.STORE_DB_TYPE);
        if (StringUtils.isBlank(dbType)) {
            throw new StoreException("there must be db type.");
        }
        if (logStoreDataSource == null) {
            throw new StoreException("there must be logStoreDataSource.");
        }
    }

    @Override
    public boolean acquireLock(LockDO lockDO) {
        return acquireLock(Collections.singletonList(lockDO));
    }

    /**
     * ====================================真正加锁的逻辑
     * @param lockDOs the lock d os
     * @return
     */
    @Override
    public boolean acquireLock(List<LockDO> lockDOs) {
        //准备数据库连接等
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        //数据库存在函数的keys
        Set<String> dbExistedRowKeys = new HashSet<>();
        boolean originalAutoCommit = true;
        if (lockDOs.size() > 1) {
            lockDOs = lockDOs.stream().filter(LambdaUtils.distinctByKey(LockDO::getRowKey)).collect(Collectors.toList());
        }
        try {
            conn = logStoreDataSource.getConnection();
            if (originalAutoCommit = conn.getAutoCommit()) {
                conn.setAutoCommit(false);
            }

            /**
             * 假如我们的分支事务 需要更新的资源是productId 为1 和2
             * 那么他的全局锁的key是product:1,2 而List<LockDO>.size为2
             * 所以下面代码拼接为(?,?) 若只有productId为1的那么他的拼接(?)
             */

            //check lock
            StringJoiner sj = new StringJoiner(",");
            for (int i = 0; i < lockDOs.size(); i++) {
                sj.add("?");
            }
            boolean canLock = true;
            //执行查询我们的lock_table
            //query
            String checkLockSQL = LockStoreSqls.getCheckLockableSql(lockTable, sj.toString(), dbType);
            ps = conn.prepareStatement(checkLockSQL);
            for (int i = 0; i < lockDOs.size(); i++) {
                ps.setString(i + 1, lockDOs.get(i).getRowKey());
            }
            rs = ps.executeQuery();
            // 获取全局事务参与者所属的分布式事务全局ID
            String currentXID = lockDOs.get(0).getXid();
            // 循环数据库
            while (rs.next()) {
                // 通过查询到数据库中的数据
                String dbXID = rs.getString(ServerTableColumnsName.LOCK_TABLE_XID);
                /**
                 * 重点  ！！！！！！！！！！！！
                 *
                 * 当前的锁和数据库中的全局事务ID不相等，说明其他的分布式事务真正对该记录进行操作
                 *  线程1对应的分布式事务 对product:1 进行加锁操作
                 * 线程2对应的分布式事务对product:1进行操作,那么去数据库查询出来的全局
                 * xid和当前线程2对应的xid是不相等的,那么就加锁失败。
                 * 对应的线程1 global-table表中的rowkey为product:1 xid(全局事务id)为abc，
                 *      线程2 global-table表中的rowkey为product:1 xid(全局事务id)为hgfff
                 *  两个事务才操作同一条数据, 会造成全局锁冲突, 导致加锁失败
                 */
                if (!StringUtils.equals(dbXID, currentXID)) {
                    if (LOGGER.isInfoEnabled()) {
                        String dbPk = rs.getString(ServerTableColumnsName.LOCK_TABLE_PK);
                        String dbTableName = rs.getString(ServerTableColumnsName.LOCK_TABLE_TABLE_NAME);
                        Long dbBranchId = rs.getLong(ServerTableColumnsName.LOCK_TABLE_BRANCH_ID);
                        LOGGER.info("Global lock on [{}:{}] is holding by xid {} branchId {}", dbTableName, dbPk, dbXID,
                            dbBranchId);
                    }
                    //取反操作，直接加锁失败
                    canLock &= false;
                    //跳出循环
                    break;
                }
                //收集数据库中操作的行锁的key
                dbExistedRowKeys.add(rs.getString(ServerTableColumnsName.LOCK_TABLE_ROW_KEY));
            }
            //加锁失败直接返回
            if (!canLock) {
                conn.rollback();
                return false;
            }
            List<LockDO> unrepeatedLockDOs = null;
            //线程1：分布式事务product:1,那么数据库加锁成功
            //后面线程1:有执行一次语句 product:1,2 那么dbExistedRowKeys[1]
            if (CollectionUtils.isNotEmpty(dbExistedRowKeys)) {
                // 进行重复数据sql的去重操作
                //lockDOs[1,2] 过滤后unrepeatedLockDOs [2]
                unrepeatedLockDOs = lockDOs.stream().filter(lockDO -> !dbExistedRowKeys.contains(lockDO.getRowKey()))
                    .collect(Collectors.toList());
            } else {
                unrepeatedLockDOs = lockDOs;
            }
            if (CollectionUtils.isEmpty(unrepeatedLockDOs)) {
                conn.rollback();
                return true;
            }
            //lock
            if (unrepeatedLockDOs.size() == 1) {
                LockDO lockDO = unrepeatedLockDOs.get(0);
                //进行加锁,说白了就是保存数据库
                // 往lock_table
                if (!doAcquireLock(conn, lockDO)) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Global lock acquire failed, xid {} branchId {} pk {}", lockDO.getXid(), lockDO.getBranchId(), lockDO.getPk());
                    }
                    conn.rollback();
                    return false;
                }
            } else {
                if (!doAcquireLocks(conn, unrepeatedLockDOs)) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Global lock batch acquire failed, xid {} branchId {} pks {}", unrepeatedLockDOs.get(0).getXid(),
                            unrepeatedLockDOs.get(0).getBranchId(), unrepeatedLockDOs.stream().map(lockDO -> lockDO.getPk()).collect(Collectors.toList()));
                    }
                    conn.rollback();
                    return false;
                }
            }
            conn.commit();
            return true;
        } catch (SQLException e) {
            throw new StoreException(e);
        } finally {
            IOUtil.close(rs, ps);
            if (conn != null) {
                try {
                    if (originalAutoCommit) {
                        conn.setAutoCommit(true);
                    }
                    conn.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    @Override
    public boolean unLock(LockDO lockDO) {
        return unLock(Collections.singletonList(lockDO));
    }

    @Override
    public boolean unLock(List<LockDO> lockDOs) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = logStoreDataSource.getConnection();
            conn.setAutoCommit(true);

            StringJoiner sj = new StringJoiner(",");
            for (int i = 0; i < lockDOs.size(); i++) {
                sj.add("?");
            }
            //batch release lock
            String batchDeleteSQL = LockStoreSqls.getBatchDeleteLockSql(lockTable, sj.toString(), dbType);
            ps = conn.prepareStatement(batchDeleteSQL);
            ps.setString(1, lockDOs.get(0).getXid());
            for (int i = 0; i < lockDOs.size(); i++) {
                ps.setString(i + 2, lockDOs.get(i).getRowKey());
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException(e);
        } finally {
            IOUtil.close(ps, conn);
        }
        return true;
    }

    @Override
    public boolean unLock(String xid, Long branchId) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = logStoreDataSource.getConnection();
            conn.setAutoCommit(true);
            //batch release lock by branch
            String batchDeleteSQL = LockStoreSqls.getBatchDeleteLockSqlByBranch(lockTable, dbType);
            ps = conn.prepareStatement(batchDeleteSQL);
            ps.setString(1, xid);
            ps.setLong(2, branchId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException(e);
        } finally {
            IOUtil.close(ps, conn);
        }
        return true;
    }

    @Override
    public boolean unLock(String xid, List<Long> branchIds) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = logStoreDataSource.getConnection();
            conn.setAutoCommit(true);
            StringJoiner sj = new StringJoiner(",");
            branchIds.stream().forEach(branchId -> sj.add("?"));
            //batch release lock by branch list
            String batchDeleteSQL = LockStoreSqls.getBatchDeleteLockSqlByBranchs(lockTable, sj.toString(), dbType);
            ps = conn.prepareStatement(batchDeleteSQL);
            ps.setString(1, xid);
            for (int i = 0; i < branchIds.size(); i++) {
                ps.setLong(i + 2, branchIds.get(i));
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StoreException(e);
        } finally {
            IOUtil.close(ps, conn);
        }
        return true;
    }

    @Override
    public boolean isLockable(List<LockDO> lockDOs) {
        Connection conn = null;
        try {
            conn = logStoreDataSource.getConnection();
            conn.setAutoCommit(true);
            if (!checkLockable(conn, lockDOs)) {
                return false;
            }
            return true;
        } catch (SQLException e) {
            throw new DataAccessException(e);
        } finally {
            IOUtil.close(conn);
        }
    }

    /**
     * Do acquire lock boolean.
     *
     * @param conn   the conn
     * @param lockDO the lock do
     * @return the boolean
     */
    protected boolean doAcquireLock(Connection conn, LockDO lockDO) {
        PreparedStatement ps = null;
        try {
            //insert
            String insertLockSQL = LockStoreSqls.getInsertLockSQL(lockTable, dbType);
            ps = conn.prepareStatement(insertLockSQL);
            ps.setString(1, lockDO.getXid());
            ps.setLong(2, lockDO.getTransactionId());
            ps.setLong(3, lockDO.getBranchId());
            ps.setString(4, lockDO.getResourceId());
            ps.setString(5, lockDO.getTableName());
            ps.setString(6, lockDO.getPk());
            ps.setString(7, lockDO.getRowKey());
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new StoreException(e);
        } finally {
            IOUtil.close(ps);
        }
    }

    /**
     * Do acquire lock boolean.
     *
     * @param conn    the conn
     * @param lockDOs the lock do list
     * @return the boolean
     */
    protected boolean doAcquireLocks(Connection conn, List<LockDO> lockDOs) {
        PreparedStatement ps = null;
        try {
            //insert
            String insertLockSQL = LockStoreSqls.getInsertLockSQL(lockTable, dbType);
            ps = conn.prepareStatement(insertLockSQL);
            for (LockDO lockDO : lockDOs) {
                ps.setString(1, lockDO.getXid());
                ps.setLong(2, lockDO.getTransactionId());
                ps.setLong(3, lockDO.getBranchId());
                ps.setString(4, lockDO.getResourceId());
                ps.setString(5, lockDO.getTableName());
                ps.setString(6, lockDO.getPk());
                ps.setString(7, lockDO.getRowKey());
                ps.addBatch();
            }
            return ps.executeBatch().length == lockDOs.size();
        } catch (SQLException e) {
            LOGGER.error("Global lock batch acquire error: {}", e.getMessage(), e);
            //return false,let the caller go to conn.rollabck()
            return false;
        } finally {
            IOUtil.close(ps);
        }
    }

    /**
     * Check lock boolean.
     *
     * @param conn    the conn
     * @param lockDOs the lock do
     * @return the boolean
     */
    protected boolean checkLockable(Connection conn, List<LockDO> lockDOs) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            StringJoiner sj = new StringJoiner(",");
            for (int i = 0; i < lockDOs.size(); i++) {
                sj.add("?");
            }

            //query
            String checkLockSQL = LockStoreSqls.getCheckLockableSql(lockTable, sj.toString(), dbType);
            ps = conn.prepareStatement(checkLockSQL);
            for (int i = 0; i < lockDOs.size(); i++) {
                ps.setString(i + 1, lockDOs.get(i).getRowKey());
            }
            rs = ps.executeQuery();
            while (rs.next()) {
                String xid = rs.getString("xid");
                if (!StringUtils.equals(xid, lockDOs.get(0).getXid())) {
                    return false;
                }
            }
            return true;
        } catch (SQLException e) {
            throw new DataAccessException(e);
        } finally {
            IOUtil.close(rs, ps);
        }
    }

    /**
     * Sets lock table.
     *
     * @param lockTable the lock table
     */
    public void setLockTable(String lockTable) {
        this.lockTable = lockTable;
    }

    /**
     * Sets db type.
     *
     * @param dbType the db type
     */
    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    /**
     * Sets log store data source.
     *
     * @param logStoreDataSource the log store data source
     */
    public void setLogStoreDataSource(DataSource logStoreDataSource) {
        this.logStoreDataSource = logStoreDataSource;
    }
}
