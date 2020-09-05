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
package io.seata.server.lock;

import java.util.ArrayList;
import java.util.List;

import io.seata.common.XID;
import io.seata.common.util.CollectionUtils;
import io.seata.common.util.StringUtils;
import io.seata.core.exception.TransactionException;
import io.seata.core.lock.Locker;
import io.seata.core.lock.RowLock;
import io.seata.server.session.BranchSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Abstract lock manager.
 *
 * @author zhangsen
 */
public abstract class AbstractLockManager implements LockManager {

    /**
     * The constant LOGGER.
     */
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractLockManager.class);

    @Override
    public boolean acquireLock(BranchSession branchSession) throws TransactionException {
        if (branchSession == null) {
            throw new IllegalArgumentException("branchSession can't be null for memory/file locker.");
        }
        //从分支事务对象中拿到 全局锁的key
        //lockKey的格式为 表名:主键id product:1
        //若是多个资源上锁 product:1,2,3.....
        String lockKey = branchSession.getLockKey();
        if (StringUtils.isNullOrEmpty(lockKey)) {
            //no lock
            return true;
        }
        /**
         * 进行 行锁收集
         * 获取全局锁
         */
        //get locks of branch
        List<RowLock> locks = collectRowLocks(branchSession);
        if (CollectionUtils.isEmpty(locks)) {
            //no lock
            return true;
        }
        /**
         * 锁冲突了, 进行尝试重新获取 锁
         * DataBaseLocker # acquireLock()
         * //通过SPI的机制去读取meta‐inf/service/io.seata.core.lock.Locker
         * 读取我们的io.seata.server.lock.db.DataBaseLocker 数据库加锁器
         * 通过io.seata.server.lock.db.DataBaseLocker 进行加锁
         */
        return getLocker(branchSession).acquireLock(locks);
    }

    @Override
    public boolean releaseLock(BranchSession branchSession) throws TransactionException {
        if (branchSession == null) {
            throw new IllegalArgumentException("branchSession can't be null for memory/file locker.");
        }
        List<RowLock> locks = collectRowLocks(branchSession);
        try {
            return getLocker(branchSession).releaseLock(locks);
        } catch (Exception t) {
            LOGGER.error("unLock error, branchSession:{}", branchSession, t);
            return false;
        }
    }

    @Override
    public boolean isLockable(String xid, String resourceId, String lockKey) throws TransactionException {
        List<RowLock> locks = collectRowLocks(lockKey, resourceId, xid);
        try {
            return getLocker().isLockable(locks);
        } catch (Exception t) {
            LOGGER.error("isLockable error, xid:{} resourceId:{}, lockKey:{}", xid, resourceId, lockKey, t);
            return false;
        }
    }


    @Override
    public void cleanAllLocks() throws TransactionException {
        getLocker().cleanAllLocks();
    }

    /**
     * Gets locker.
     *
     * @return the locker
     */
    protected Locker getLocker() {
        return getLocker(null);
    }

    /**
     * Gets locker.
     *
     * @param branchSession the branch session
     * @return the locker
     */
    protected Locker getLocker(BranchSession branchSession) {
        return LockerFactory.get(branchSession);
    }

    /**
     * Collect row locks list.`
     *
     * @param branchSession the branch session
     * @return the list
     */
    protected List<RowLock> collectRowLocks(BranchSession branchSession) {
        List<RowLock> locks = new ArrayList<>();
        if (branchSession == null || StringUtils.isBlank(branchSession.getLockKey())) {
            return locks;
        }
        //获取全局ID 192.168.159.1:8091:2029808902
        // 分支会话 从 branch_table获取全局事务id
        String xid = branchSession.getXid();
        // //jdbc:mysql://localhost:3306/seata‐product
        String resourceId = branchSession.getResourceId();
        //2029808902
        long transactionId = branchSession.getTransactionId();
        //拿到全局锁
        String lockKey = branchSession.getLockKey();
        //封装行锁对象,就是把product:1,2封装成二个行锁对象
        return collectRowLocks(lockKey, resourceId, xid, transactionId, branchSession.getBranchId());
    }

    /**
     * Collect row locks list.
     *
     * @param lockKey    the lock key
     * @param resourceId the resource id
     * @param xid        the xid
     * @return the list
     */
    protected List<RowLock> collectRowLocks(String lockKey, String resourceId, String xid) {
        return collectRowLocks(lockKey, resourceId, xid, XID.getTransactionId(xid), null);
    }

    /**
     * Collect row locks list.
     *
     * @param lockKey       the lock key
     * @param resourceId    the resource id
     * @param xid           the xid
     * @param transactionId the transaction id
     * @param branchID      the branch id
     * @return the list
     */
    //封装行锁对象,就是把product:1,2封装成二个行锁对象
    protected List<RowLock> collectRowLocks(String lockKey, String resourceId, String xid, Long transactionId,
                                            Long branchID) {
        List<RowLock> locks = new ArrayList<RowLock>();

        String[] tableGroupedLockKeys = lockKey.split(";");
        for (String tableGroupedLockKey : tableGroupedLockKeys) {
            int idx = tableGroupedLockKey.indexOf(":");
            if (idx < 0) {
                return locks;
            }
            // //获取表名 product
            String tableName = tableGroupedLockKey.substring(0, idx);
            // 获取主键
            String mergedPKs = tableGroupedLockKey.substring(idx + 1);
            if (StringUtils.isBlank(mergedPKs)) {
                return locks;
            }
            String[] pks = mergedPKs.split(",");
            if (pks == null || pks.length == 0) {
                return locks;
            }
            for (String pk : pks) {
                if (StringUtils.isNotBlank(pk)) {
                    RowLock rowLock = new RowLock();
                    rowLock.setXid(xid);
                    rowLock.setTransactionId(transactionId);
                    rowLock.setBranchId(branchID);
                    rowLock.setTableName(tableName);
                    rowLock.setPk(pk);
                    rowLock.setResourceId(resourceId);
                    locks.add(rowLock);
                }
            }
        }
        return locks;
    }

}
