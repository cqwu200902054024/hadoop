package net.cqwu.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments: HBase 操作工具
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu.util
 *
 * @author： p.z
 * Create Date：  2018-06-11
 * Modified By：   p.z
 * Modified Date:  2018-06-11
 * Why & What is modified
 * Version:        V1.0
 */
public class HBaseHelper implements Closeable {
    private Configuration configuration;
    private Connection connection;
    private Admin admin;

    public HBaseHelper(Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.connection = ConnectionFactory.createConnection(this.configuration);
        this.admin = this.connection.getAdmin();
    }

    public static HBaseHelper getHbaseHelper(Configuration configuration) throws IOException {
        return new HBaseHelper(configuration);
    }

    @Override
    public void close() throws IOException {
        this.connection.close();
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public Connection getConnection() {
        return connection;
    }

    public Admin getAdmin() {
        return admin;
    }

    public void createNamespace(String namespace) {
        try {
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            this.admin.createNamespace(namespaceDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void dropNamespace(String namespace,boolean force) {
        //this.admin.deleteNamespace();
    }
}
