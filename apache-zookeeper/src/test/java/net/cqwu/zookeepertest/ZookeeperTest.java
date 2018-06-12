package net.cqwu.zookeepertest;

import net.cqwu.hadoopbook.*;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments:
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu.zookeepertest
 *
 * @author： Administrator
 * Create Date：  2018-06-12
 * Modified By：   Administrator
 * Modified Date:  2018-06-12
 * Why & What is modified
 * Version:        V1.0
 */
public class ZookeeperTest {
    private CreateGroup createGroup;
    private JoinGroup joinGroup;
    private ListGroup listGroup;
    private DeleteGroup deleteGroup;
    private ActiveKeyValueStore activeKeyValueStore;
    private ConfigWatcher configWatcher;

    @Before
    public void init() throws IOException, InterruptedException {
        this.createGroup = new CreateGroup();
        this.createGroup.connect("master:2181");
        this.joinGroup = new JoinGroup();
        this.joinGroup.connect("master:2181");
        this.listGroup = new ListGroup();
        this.listGroup.connect("master:2181");
        this.deleteGroup = new DeleteGroup();
        this.deleteGroup.connect("master:2181");
        this.activeKeyValueStore = new ActiveKeyValueStore();
        this.activeKeyValueStore.connect("master:2181");
        this.configWatcher = new ConfigWatcher("master:2181");
    }

    @Test
    public void testZK01() throws IOException, InterruptedException,KeeperException {
        System.out.println(this.createGroup.create("config"));
        this.createGroup.close();
    }

    @Test
    public void testZK02() throws KeeperException, InterruptedException {
        for(int i = 0; i < 20; i ++) {
            this.joinGroup.join("group","member" + i + "_");
        }
    }

    @Test
    public void testZK03() throws KeeperException, InterruptedException {
        while (true) {
            this.listGroup.list("group");
            Thread.sleep(500);
        }
    }

    @Test
    public void testZK04() throws KeeperException, InterruptedException {
         this.deleteGroup.delete("group");

    }

    @Test
    public void testZK05() throws KeeperException, InterruptedException {
        int i = 10000;
    while(true) {
        this.activeKeyValueStore.write("key" + 1,"value" + i);
        i ++;
        Thread.sleep(500);
    }

   /*     for(int i = 0; i < 100; i ++) {
            this.activeKeyValueStore.write("key" + i,"value" + i);
            Thread.sleep(500);
        }*/
    }

    @Test
    public void testZK06() throws InterruptedException, IOException, KeeperException {
           // this.configWatcher = new ConfigWatcher("master:2181");
            this.configWatcher.setWatcherkey("key1");
            this.configWatcher.display(this.configWatcher.getWatcherkey());
            Thread.sleep(Long.MAX_VALUE);
    }
}
