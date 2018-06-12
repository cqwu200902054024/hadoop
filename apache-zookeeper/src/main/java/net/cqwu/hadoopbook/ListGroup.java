package net.cqwu.hadoopbook;

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
public class ListGroup {
    private static final int SESSION_TIMEOUT = 5000;
    private ZooKeeper zooKeeper;

    public void connect(String hosts) throws IOException {
        this.zooKeeper = new ZooKeeper(hosts,SESSION_TIMEOUT,null);
    }
    public void list(String groupName) throws KeeperException, InterruptedException {
        String path = "/" + groupName;
        List<String> childrens = this.zooKeeper.getChildren(path,new ZnodeWatcher());
        for(String children : childrens) {
            System.out.println(children);
        }
    }
}
