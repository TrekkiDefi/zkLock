package com.fnpac.zk.lock;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by 刘春龙 on 2017/10/16.
 */
public class ZkLockTest {

    private final Logger logger = LoggerFactory.getLogger(ZkLockTest.class);

    private String connectString = "localhost:2181";
    private String namespace = "LockServiceCenter";

    // 这里为了方便测试，锁的超时等待时间设置最大值，生产环境建议根据情况合理设置
    private Long timeout = Long.MAX_VALUE;

    /**
     * 排他锁
     *
     * @throws InterruptedException
     */
    @Test
    public void exclusiveLockTest() throws InterruptedException {

        String lockKey = "lockKey_127.0.0.1";
        ZkLock zkLock = new ZkLock(connectString, namespace, lockKey, timeout);

        zkLock.getExclusiveLock();
        try {
            logger.info("锁内业务输出...");
            Thread.sleep(30000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            zkLock.unlockForExclusive();
        }
        Thread.sleep(10000L);
    }

    /**
     * 共享读锁
     *
     * @throws InterruptedException
     */
    @Test
    public void rSharedLockTest() throws InterruptedException {

        String lockKey = "lockKey_127.0.0.1";
        ZkLock zkLock = new ZkLock(connectString, namespace, lockKey, timeout);

        zkLock.getSharedLock(ZkLock.SharedLockType.read);
        try {
            logger.info("锁内业务输出...");
            Thread.sleep(30000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            zkLock.unlockForShardLock();
        }
        Thread.sleep(10000L);
    }

    /**
     * 共享写锁
     *
     * @throws InterruptedException
     */
    @Test
    public void wSharedLockTest() throws InterruptedException {

        String lockKey = "lockKey_127.0.0.1";
        ZkLock zkLock = new ZkLock(connectString, namespace, lockKey, timeout);

        zkLock.getSharedLock(ZkLock.SharedLockType.write);
        try {
            logger.info("锁内业务输出...");
            Thread.sleep(30000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            zkLock.unlockForShardLock();
        }
        Thread.sleep(10000L);
    }
}
