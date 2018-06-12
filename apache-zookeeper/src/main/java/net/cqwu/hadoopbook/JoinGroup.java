package net.cqwu.hadoopbook;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments: 创建临时节点，每个临时节点代表一个应用。
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
public class JoinGroup extends ConnectionWatcher {
    public String join(String groupName,String member) throws KeeperException, InterruptedException {
        String path = "/" + groupName + "/" +member;
        return  this.zooKeeper.create(path,member.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    }
}
