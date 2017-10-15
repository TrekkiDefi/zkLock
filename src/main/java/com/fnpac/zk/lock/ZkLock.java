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

import java.util.*;
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
 * /{namespace}/SharedLock/{lockKey}/R{UUID}00000000<br>
 * /{namespace}/SharedLock/{lockKey}/R{UUID}00000001<br>
 * /{namespace}/SharedLock/{lockKey}/R{UUID}00000002<br>
 * <p>
 * 写：<br>
 * /{namespace}/SharedLock/{lockKey}/W{UUID}00000000<br>
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
    private CountDownLatch latch = new CountDownLatch(1);
    private final String lockKey;// 当前锁的Key
    private final Long timeout;// 超时等待时间
    private CountDownLatch shardLocklatch = new CountDownLatch(1);
    private String selfIdentity = null;// 共享锁 当前所有者的标识
    private String selfNodeName = null;// 共享锁 当前所有者对应的节点名称

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

            // 创建共享锁目录
            if (client.checkExists().forPath("/" + SharedLock) == null) {
                client.create().creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath("/" + SharedLock);
            }
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
                        // 判断不是当前锁的释放，收到自己的通知就不处理
                        if (path.contains(selfIdentity))
                            return;

                        List<String> lockChildrens = client.getChildren().forPath(
                                "/" + SharedLock + "/" + lockKey);// 获取当前共享锁下所有的读些锁

                        boolean isLock = false;
                        if (selfIdentity.startsWith("R")) {
                            isLock = canGetLock(lockChildrens, 0, true);
                        } else if (selfIdentity.startsWith("W")) {
                            isLock = canGetLock(lockChildrens, 1, true);
                        }

                        logger.info("收到共享锁释放通知后，重新尝试获取锁，结果为:" + isLock);
                        if (isLock) {
                            //获得锁
                            logger.info("获得共享锁，解除因为获取不到锁的阻塞");
                            shardLocklatch.countDown();
                        }
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
                    logger.error("当前线程中断，获取排他锁失败", ie);
                    return false;
                }
            }
        }
    }

    /**
     * 排他锁解锁
     *
     * @return
     */
    public boolean unlockForExclusive() {
        try {
            if (client.checkExists().forPath("/" + ExclusiveLock + "/" + this.lockKey) != null) {
                client.delete().deletingChildrenIfNeeded().forPath("/" + ExclusiveLock + "/" + this.lockKey);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 获取共享锁
     * <p>
     * 共享锁采用的是阻塞获取锁的方式
     *
     * @param lockType 锁类型，读 or 写，0为读锁，1为写锁
     * @return
     */
    public boolean getSharedLock(final Enum lockType) {
        return getSharedLock(lockType, null);
    }

    /**
     * 获取共享锁
     * <p>
     * 共享锁采用的是阻塞获取锁的方式
     *
     * @param lockType 锁类型，读 or 写，0为读锁，1为写锁
     * @param identity 获取当前锁的所有者的身份标识。为null则默认使用uuid
     * @return
     */
    public boolean getSharedLock(final Enum lockType, String identity) {

        if (lockType == null || (lockType.ordinal() != 0 && lockType.ordinal() != 1)) {
            throw new IllegalArgumentException("lockType参数非法");
        }
        if (StringUtils.isEmpty(identity)) {
            identity = UUID.randomUUID().toString();
        }
        if (identity.contains("@") || identity.contains("/")) {
            throw new IllegalArgumentException("identity不能包含字符@、/");
        }

        String nodeNamePrefix = null;// 锁节点名称前缀
        if (lockType.ordinal() == 0) {
            nodeNamePrefix = "R" + identity + "@";
        } else if (lockType.ordinal() == 1) {
            nodeNamePrefix = "W" + identity + "@";
        }
        selfIdentity = nodeNamePrefix.substring(0, nodeNamePrefix.length() - 1);// 共享锁当前所有者的标识

        try {
            selfNodeName = client.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath("/" + SharedLock + "/" + this.lockKey + "/" + nodeNamePrefix);

            logger.info("创建共享锁子节点 - " + selfNodeName);

            List<String> lockChildrens = client.getChildren().forPath(
                    "/" + SharedLock + "/" + this.lockKey);// 获取当前共享锁下所有的读些锁
            if (!canGetLock(lockChildrens, lockType.ordinal(), false)) {
                shardLocklatch.await();
            }
        } catch (Exception e) {
            logger.error("获取共享锁出错", e);
            e.printStackTrace();
            return false;
        }
        logger.info("获取共享锁成功");
        return true;
    }

    /**
     * 共享锁解锁
     *
     * @return
     */
    public boolean unlockForShardLock() {
        try {
            if (client.checkExists().forPath(selfNodeName) != null) {
                client.delete().forPath(selfNodeName);
            }

            List<String> lockChildrens = client.getChildren().forPath(
                    "/" + SharedLock + "/" + this.lockKey);// 获取当前共享锁下所有的读些锁

            if (lockChildrens.size() == 0) {
                if (client.checkExists().forPath("/" + SharedLock + "/" + this.lockKey) != null) {
                    client.delete().inBackground().forPath("/" + SharedLock + "/" + this.lockKey);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private boolean canGetLock(List<String> childrens, int type, boolean reps) {
        if (childrens.size() == 0)// 没有其他读写锁，获取锁成功
            return true;

        try {
            String currentSeq = null;
            List<String> sortSeqs = new ArrayList<String>();
            Map<String, String> seqs_identitys = new HashMap<String, String>();
            for (String child : childrens) {
                String splits[] = child.split("@");
                sortSeqs.add(splits[1]);// 节点序号，eg，00000000 00000001 00000002

                seqs_identitys.put(splits[1], splits[0]);

                if (this.selfIdentity.equals(splits[0]))
                    currentSeq = splits[1];
            }

            Collections.sort(sortSeqs);

            if (currentSeq.equals(sortSeqs.get(0))) {// 当前节点是第一个节点，则无论是读锁还是写锁都可以获取
                logger.info("获取共享锁成功，因为是第一个获取锁的请求，所以获取成功");
                return true;
            } else {// 当前节点不是第一个节点
                if (type == 1) {// 写锁
                    // 第一次请求获取锁则设置监听，以后就不设置了，因为监听一直存在
                    if (!reps)
                        addChildWatcher(SharedLock, this.lockKey);
                    logger.info("获取共享写锁失败，因为前面有其它读写锁，所以获取失败");
                    return false;
                } else if (type == 0) {// 读锁

                    boolean hasW = false;
                    for (String seq : sortSeqs) {
                        if (seq.equals(currentSeq)) {
                            break;
                        }
                        if (seqs_identitys.get(seq).startsWith("W")) {
                            hasW = true;
                            break;
                        }
                    }

                    if (!hasW) {// 前面不存在写锁，获取锁成功
                        return true;
                    } else {// 前面存在写锁，需要等待
                        // 第一次请求获取锁则设置监听，以后就不设置了，因为监听一直存在
                        if (!reps)
                            addChildWatcher(SharedLock, this.lockKey);
                        logger.info("获取共享读锁失败，因为前面有其它写锁，所以获取失败");
                        return false;
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public enum SharedLockType {
        read, write;

        SharedLockType() {
        }
    }
}
