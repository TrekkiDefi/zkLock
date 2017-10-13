package com.fnpac.zk.lock;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by 刘春龙 on 2017/10/13.
 * <p>
 * eg，
 * <p>
 * 排他锁：/{namespace}/ExclusiveLock/{lockKey}/Lock
 * <p>
 * 共享锁：
 * <p>
 * 读：<br>
 * /{namespace}/SharedLock/{lockKey}/RLock00000000<br>
 * /{namespace}/SharedLock/{lockKey}/RLock00000001<br>
 * /{namespace}/SharedLock/{lockKey}/RLock00000002<br>
 * <p>
 * 写：<br>
 * /{namespace}/SharedLock/{lockKey}/WLock00000000<br>
 */
public class ZkLock {

    private static final Logger logger = LoggerFactory.getLogger(ZkLock.class);

    /**
     * Global static variable
     */
    private static CuratorFramework client = null;
    private static final String ExclusiveLock = "ExclusiveLock";// 排他锁目录
    private static final String SharedLock = "SharedLock";// 共享锁目录
    private static final String Lock = "Lock";

    /**
     * Current lock variable
     */
    private final String lockKey;// 当前锁的Key
    private CountDownLatch latch = new CountDownLatch(1);
    private final Long timeout;// 超时等待时间

    /**
     * 构造锁对象
     *
     * @param connectString zookeeper连接地址
     * @param namespace     命名空间
     * @param lockKey       当前锁的Key
     * @param timeout       超时等待时间
     */
    public ZkLock(String connectString, String namespace, String lockKey, Long timeout) {
        if (StringUtils.isEmpty(connectString)) {
            throw new IllegalArgumentException("Zookeeper connectString is Null");
        }
        if (StringUtils.isEmpty(namespace)) {
            throw new IllegalArgumentException("Zookeeper namespace is Null");
        }
        if (StringUtils.isEmpty(lockKey)) {
            throw new IllegalArgumentException("Lock key is Null");
        }
        if (timeout == null) {
            throw new IllegalArgumentException("Lock timeout is Null");
        }

        this.lockKey = lockKey;
        this.timeout = timeout;

        // 创建zookeeper客户端连接，并初始化排他锁目录和共享锁目录
        init(connectString, namespace);

        // 添加排他锁watcher
        try {
            addChildWatcher(ExclusiveLock, this.lockKey);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    /**
     * 创建zookeeper客户端连接，并初始化排他锁目录和共享锁目录
     *
     * @param connectString zookeeper连接地址
     * @param namespace     命名空间
     */
    private static synchronized void init(String connectString, String namespace) {
        if (client != null)
            return;

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);// 重试策略
        client = CuratorFrameworkFactory.builder().connectString(connectString)
                .sessionTimeoutMs(10000).retryPolicy(retryPolicy)
                .namespace(namespace).build();
        client.start();

        // 创建锁目录
        try {
            // 创建排他锁目录
            if (client.checkExists().forPath("/" + ExclusiveLock) == null) {
                client.create().creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                        .forPath("/" + ExclusiveLock);
            }

            // TODO 创建共享锁目录
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 监听当前锁的子节点的创建、删除、节点数据变化
     *
     * @param lockDir 锁目录
     * @param lockKey 当前锁的Key
     * @throws Exception
     */
    private void addChildWatcher(String lockDir, final String lockKey) throws Exception {
        // 监听当前锁的子节点的创建、删除、节点数据变化
        final PathChildrenCache cache = new PathChildrenCache(client, "/" + lockDir + "/" + lockKey,
                true);
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

        cache.getListenable().addListener(new PathChildrenCacheListener() {

            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                if (event.getType().equals(
                        PathChildrenCacheEvent.Type.INITIALIZED)) {

                } else if (event.getType().equals(
                        PathChildrenCacheEvent.Type.CHILD_ADDED)) {

                } else if (event.getType().equals(
                        PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                    // 有锁释放
                    String path = event.getData().getPath();

                    if (path.startsWith("/" + ExclusiveLock + "/" + lockKey)) {
                        logger.info("排他锁,收到锁释放通知！path：" + path);
                        // 其它排他锁释放，countDown使当前线程重新去争夺锁（排他锁采用的是循环阻塞获取锁的方式）
                        latch.countDown();
                    } else if (path.startsWith("/" + SharedLock + "/" + lockKey)) {
                        logger.info("共享锁,收到锁释放通知！path：" + path);
                        // 其它共享锁释放，重新去争夺锁，如果成功获取锁，则countDown使当前线程返回获取锁成功（共享锁采用的是阻塞获取锁的方式）
                        // 判断是不是当前锁的释放
                        // TODO
                    }
                } else if (event.getType().equals(
                        PathChildrenCacheEvent.Type.CHILD_UPDATED)) {

                }
            }
        });
    }

    /**
     * 获取排他锁
     * <p>
     * 排他锁采用的是循环阻塞获取锁的方式
     */
    public boolean getExclusiveLock() {
        while (true) {
            try {
                client.create().creatingParentsIfNeeded()
                        .withMode(CreateMode.EPHEMERAL)
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                        .forPath("/" + ExclusiveLock + "/" + this.lockKey + "/" + Lock);

                logger.info("获取排他锁成功");
                return true;// 如果节点创建成功，即说明获取排他锁成功，返回true
            } catch (Exception e) {
                logger.info("获取排他锁失败");
                // 如果没有获取到排他锁，需要重新设置同步资源值
                if (latch.getCount() <= 0) {
                    latch = new CountDownLatch(1);
                }
                try {
                    boolean rs = latch.await(this.timeout, TimeUnit.MILLISECONDS);
                    if (!rs) {// false，等待超时
                        logger.info("获取排他锁等待超时，获取锁失败!~");
                        return false;
                    }
                } catch (InterruptedException ie) {
                    logger.error("线程中断，获取排他锁失败", ie);
                    return false;
                }
            }
        }
    }
}
