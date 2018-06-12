package net.cqwu.hadoopbook;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments: 配置服务
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
public class ActiveKeyValueStore extends ConnectionWatcher {
    private static final String ROOTPATH = "/config/";

    /**
     * 将数据以键值对的形式存入zookeeper
     * @param key
     * @param value
     */
    public void write(String key,String value) throws KeeperException, InterruptedException {
        Stat stat = this.zooKeeper.exists(ROOTPATH + key,null);
        if(stat == null) {
            this.zooKeeper.create(ROOTPATH + key, value.getBytes(Charset.forName("UTF-8")), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            this.zooKeeper.setData(ROOTPATH + key,value.getBytes(Charset.forName("UTF-8")),-1);
        }
    }

    public String read(String key, Watcher watcher) throws KeeperException, InterruptedException, UnsupportedEncodingException {
        String value = new String(this.zooKeeper.getData(ROOTPATH + key,watcher,null),"UTF-8");
        return value;
    }
}
