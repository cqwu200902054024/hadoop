package net.cqwu.hadoopbook;

import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments:
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu.hadoopbook
 *
 * @author： Administrator
 * Create Date：  2018-06-12
 * Modified By：   Administrator
 * Modified Date:  2018-06-12
 * Why & What is modified
 * Version:        V1.0
 */
public class DeleteGroup {
    private static final int SESSION_TIMEOUT = 5000;
    private ZooKeeper zk;

    public void connect(String hosts) throws IOException {
        this.zk = new ZooKeeper(hosts,SESSION_TIMEOUT,null);
    }

    public void delete(String groupName) throws KeeperException, InterruptedException {
        String path = "/" + groupName;
        List<String> childrens = this.zk.getChildren(path,new ZnodeWatcher());
        for(String children : childrens) {
            this.zk.delete(path + "/" + children,-1);
            Thread.sleep(500);
        }
        Thread.sleep(500);
        this.zk.delete(path,-1);
    }

    public void close() throws InterruptedException {
        this.zk.close();
    }
}
