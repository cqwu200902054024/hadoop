package net.cqwu.hadoopbook;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments: 配置变动观察者
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
public class ConfigWatcher implements Watcher {
    private ActiveKeyValueStore activeKeyValueStore;
    /**
     * 需要监视的key
     */
    private String watcherkey;

    public void setWatcherkey(String watcherkey) {
        this.watcherkey = watcherkey;
    }

    public String getWatcherkey() {
        return watcherkey;
    }

    public ConfigWatcher(String hosts) throws IOException, InterruptedException {
        this.activeKeyValueStore = new ActiveKeyValueStore();
        this.activeKeyValueStore.connect(hosts);
    }

    /**
     * 显示配置信息
     */
    public void display(String key) throws InterruptedException, UnsupportedEncodingException, KeeperException {
        String value = this.activeKeyValueStore.read(key,this);
        System.out.println(key + "<--->" + value);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
           if(watchedEvent.getType() == Event.EventType.NodeDataChanged) {
               try {
                   display(this.watcherkey);
               } catch (InterruptedException e) {
                   e.printStackTrace();
               } catch (UnsupportedEncodingException e) {
                   e.printStackTrace();
               } catch (KeeperException e) {
                   e.printStackTrace();
               }
           }
    }
}
