package net.cqwu.hadoopbook;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments: 在zookeeper中新建组成员
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu.hadoopbook
 *
 * @author： p.z
 * Create Date：  2018-06-12
 * Modified By：   Administrator
 * Modified Date:  2018-06-12
 * Why & What is modified
 * Version:        V1.0
 */
public class CreateGroup implements Watcher{
    private static final int SESSION_TIMEOUT = 5000;
    private ZooKeeper zk;
    /**
     * 阻止使用新建的Zookeeper直到这个zookeeper准备就绪。
     */
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * 创建一个zookeeper实例
     * @param hosts
     */
    public void connect(String hosts) throws IOException, InterruptedException {
        this.zk = new ZooKeeper(hosts,SESSION_TIMEOUT,this);
        //同步创建
        this.countDownLatch.await();
    }

    /**
     * 监听接口:监听zookeeper状态
     * 用于接受来自zookeeper的回调，获得各种事件通知
     * 对构造函数的调用是立即返回的，在新建ZooKeeper对象前等待其余
     * Zookpeer服务之间成功连接
     * 当客户端与服务建立连接后被调用
     * @param watchedEvent
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent.getState() + "-----");
        //创建成功
      if(watchedEvent.getState() == Event.KeeperState.SyncConnected) {
         //调用一次后计数器为0，则await方法返回
          this.countDownLatch.countDown();
      }
    }

    /**
     * 创建组(永久节点)
     * @param groupName
     */
    public String create(String groupName) throws KeeperException, InterruptedException {
        String path = "/" + groupName;
       return this.zk.create(path,"test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void close() throws InterruptedException {
        this.zk.close();
    }
}
