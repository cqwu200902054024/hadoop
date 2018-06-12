package net.cqwu.clientApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments:
 *Hbase过滤器：
 * 1.Hbase自带过滤器
 * 2.自定义过滤器（通过继承Filter）
 *3.所有过滤器都在服务端生效（谓词下推），保证过滤掉的数据不会被传送到客户端。
 * 4.过滤器在客户端创建，通过RPC传送到服务端，并在服务端生效。
 *5.过滤器的继承层次：Filter/FilterBase
 *
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu.clientApi
 *
 * @author： p.z
 * Create Date：  2018-06-11
 * Modified By：   p.z
 * Modified Date:  2018-06-11
 * Why & What is modified
 * Version:        V1.0
 */
public class HbaseFilter {
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        HbaseHelper
    }


    /**
     * 比较过滤器：行过滤器
     * @return
     */
    public static Result getResultByRowFilter() {
    }
}
