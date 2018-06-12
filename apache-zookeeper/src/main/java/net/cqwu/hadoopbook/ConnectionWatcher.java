package net.cqwu.hadoopbook;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments: zookeeper监听器（等待与zookeeper建立连接）
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu.hadoopbook
 *
 * @author： p.z
 * Create Date：  2018-06-12
 * Modified By：   p.z
 * Modified Date:  2018-06-12
 * Why & What is modified
 * Version:        V1.0
 */
public class ConnectionWatcher implements Watcher {
    private static final int SESSION_TIMEOUT = 5000;
    protected ZooKeeper zooKeeper;
    //新建一个计数器用于阻止新建Zookeeper（当前zookeeper等待连接前）
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    public void connect(String hosts) throws IOException, InterruptedException {
       this.zooKeeper = new ZooKeeper(hosts,SESSION_TIMEOUT,this);
       this.connectedSignal.await();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("zookeeper状态------>" + watchedEvent.getState());
        if(watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            this.connectedSignal.countDown();
        }
    }

    public void close() throws InterruptedException {
        this.zooKeeper.close();
    }
}
