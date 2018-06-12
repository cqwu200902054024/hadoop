package net.cqwu.hadoopbook;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

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
public class ZnodeWatcher implements Watcher {

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("节点观察--->" + watchedEvent.getType() + "  " + watchedEvent.getPath());
    }
}
