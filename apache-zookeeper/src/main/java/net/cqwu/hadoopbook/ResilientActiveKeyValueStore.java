package net.cqwu.hadoopbook;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments: 可靠配置，幂等操作
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
public class ResilientActiveKeyValueStore extends ConnectionWatcher {
    public void write(String key,String value) throws KeeperException, InterruptedException {
           int retries = 0;
           while (true) {
               try {

                   Stat stat = this.zooKeeper.exists(key, null);
                   if (stat == null) {
                       this.zooKeeper.create(key, value.getBytes(Charset.forName("UTF-8")), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                   } else {
                       this.zooKeeper.setData(key, value.getBytes(Charset.forName("UTF-8")), -1);
                   }
                   return;
               } catch (KeeperException.ConnectionLossException e) {
                   throw e;
               } catch (KeeperException e) {
                   //重试3次
                   if(retries ++ == 3) {
                        throw e;
                   }
               }
               TimeUnit.SECONDS.sleep(10);
           }
    }
}
