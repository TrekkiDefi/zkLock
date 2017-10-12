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

/**
 * Created by 刘春龙 on 2017/10/12.
 */
public class ZkLock {

    private static final Logger logger = LoggerFactory.getLogger(ZkLock.class);

    private static CuratorFramework client = null;
    private static final String ExclusiveLock = "ExclusiveLock";// 排他锁目录
    private static final String ShardLock = "ShardLock";// 共享锁目录

    public ZkLock(String connectString, String namespace) {
        if (StringUtils.isEmpty(connectString)) {
            throw new IllegalArgumentException("zookeeper connectString is Null");
        }
        if (StringUtils.isEmpty(namespace)) {
            throw new IllegalArgumentException("zookeeper namespace is Null");
        }

        init(connectString, namespace);
    }

    private void init(String connectString, String namespace) {
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
            // 添加排他锁watcher
            addChildWatcher("/" + ExclusiveLock);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 监听子节点的创建、删除、节点数据变化
     *
     * @param path
     * @throws Exception
     */
    private void addChildWatcher(String path) throws Exception {
        // 监听子节点的创建、删除、节点数据变化
        final PathChildrenCache cache = new PathChildrenCache(client, path,
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

                    if(path.startsWith("/" + ExclusiveLock)) {
                        logger.info("排他锁,收到锁释放通知！锁：" + path);
                        // 其它排他锁释放，countDown使当前线程重新去争夺锁（排他锁采用的是循环阻塞获取锁的方式）
                        // 判断不是当前锁的释放
                        // TODO
                    } else if(path.startsWith("/" + ShardLock)) {
                        logger.info("共享锁,收到锁释放通知！锁：" + path);
                        // 其它共享锁释放，重新去争夺锁，如果成功获取锁，则countDown使当前线程返回获取锁成功（共享锁采用的是阻塞获取锁的方式）
                        // 判断不是当前锁的释放
                        // TODO
                    }
                } else if (event.getType().equals(
                        PathChildrenCacheEvent.Type.CHILD_UPDATED)) {

                }
            }
        });
    }
}
