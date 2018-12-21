package com.nettyrpc.registry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 服务注册
 *
 * @author huangyong
 * @author luxiaoxun
 */
public class ServiceRegistry {

    private static final Logger logger = LoggerFactory.getLogger(ServiceRegistry.class);

    private CountDownLatch latch = new CountDownLatch(1);

    private String registryAddress;

    public ServiceRegistry(String registryAddress) {
        this.registryAddress = registryAddress;
    }

    public void register(String data) {
        if (data != null) {
            ZooKeeper zk = connectServer();
            if (zk != null) {
                AddRootNode(zk); // Add root node if not exist
                createNode(zk, data);
                CheckServiceAlive(zk,data);
            }
        }
    }

    private ZooKeeper connectServer() {
        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper(registryAddress, Constant.ZK_SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        latch.countDown();
                    }
                }
            });
            latch.await();
        } catch (IOException e) {
            logger.error("", e);
        }
        catch (InterruptedException ex){
            logger.error("", ex);
        }
        return zk;
    }

    private void AddRootNode(ZooKeeper zk){
        try {
            Stat s = zk.exists(Constant.ZK_REGISTRY_PATH, false);
            if (s == null) {
                zk.create(Constant.ZK_REGISTRY_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            logger.error(e.toString());
        } catch (InterruptedException e) {
            logger.error(e.toString());
        }
    }

    private void createNode(ZooKeeper zk, String data) {
        try {
            byte[] bytes = data.getBytes();
            String path = zk.create(Constant.ZK_DATA_PATH, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            logger.debug("create zookeeper node ({} => {})", path, data);
        } catch (KeeperException e) {
            logger.error("", e);
        }
        catch (InterruptedException ex){
            logger.error("", ex);
        }
    }

    private void CheckServiceAlive(ZooKeeper zk, String data) {
        ScheduledExecutorService scheduled = new ScheduledThreadPoolExecutor(1);
        scheduled.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                logger.debug("check {} service alive",data);
                try {
                    List<String> allChildren = zk.getChildren(Constant.ZK_REGISTRY_PATH,false);
                    List<String> allService = new ArrayList<>();
                    for (String nodePath : allChildren) {
                        byte[] aliveServiceAddress = zk.getData(Constant.ZK_REGISTRY_PATH + "/" + nodePath,false,null);
                        allService.add(new String(aliveServiceAddress));
                    }

                    if (!allService.contains(data)) {
                        zk.create(Constant.ZK_DATA_PATH,data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                        logger.warn("check service {} not alive, add into zookeeper",data);
                    }
                } catch (KeeperException e) {
                    logger.error("",e);
                } catch (InterruptedException e) {
                    logger.error("",e);
                }
            }
        },Constant.ZK_CHECK_ALIVE,Constant.ZK_CHECK_ALIVE, TimeUnit.SECONDS);
    }
}