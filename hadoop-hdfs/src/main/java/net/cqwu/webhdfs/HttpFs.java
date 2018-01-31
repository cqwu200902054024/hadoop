package net.cqwu.webhdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.net.URI;
import java.util.HashMap;
import java.util.List;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments:
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu.webhdfs
 *
 * @author： Administrator
 * Create Date：  2018-01-04
 * Modified By：   Administrator
 * Modified Date:  2018-01-04
 * Why & What is modified
 * Version:        V1.0
 */
public class HttpFs {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        //UserGroupInformation.createRemoteUser("hadoop");
        WebHdfsFileSystem webHdfsFileSystem = new WebHdfsFileSystem();
        try {
            //List
            webHdfsFileSystem.initialize(new URI("http://222.186.30.56:14000"), configuration);
            System.out.println(webHdfsFileSystem.getUri());
            //向HDFS Put文件
            // webHdfsFileSystem.copyFromLocalFile(new Path("/Users/fayson/Desktop/run-kafka/"), new Path("/fayson1-httpfs"));
            //列出HDFS根目录下的所有文件
            FileStatus[] fileStatuses =  webHdfsFileSystem.listStatus(new Path("/hive"));
            for (FileStatus fileStatus : fileStatuses) {
                System.out.println(fileStatus.getPath().getName());
            }
            webHdfsFileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}