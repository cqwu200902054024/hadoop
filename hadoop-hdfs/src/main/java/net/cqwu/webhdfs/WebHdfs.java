package net.cqwu.webhdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;

import java.net.URI;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments: 通过外网通过WebHdfs访问hdfs
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
public class WebHdfs{
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        WebHdfsFileSystem webHdfsFileSystem = new WebHdfsFileSystem();
        try {

            webHdfsFileSystem.initialize(new URI("http://222.186.52.100:50070/webhdfs/v1"), configuration);
            System.out.println(webHdfsFileSystem.getUri() + "====");
            //向HDFS Put文件
           // webHdfsFileSystem.copyFromLocalFile(new Path("/Users/fayson/Desktop/run-kafka"), new Path("/fayson1"));
            //列出HDFS根目录下的所有文件
            FileStatus[] fileStatuses =  webHdfsFileSystem.listStatus(new Path("/"));
            for (FileStatus fileStatus : fileStatuses) {
                System.out.println(fileStatus.getPath().getName());
            }
            webHdfsFileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取某个路径下所有文件
     * @param path
     * @return
     */
    public static FileStatus[] getAllFileByPath(Path path) {
        Configuration configuration = new Configuration();
        String uri = String.format("webhdfs://%s:%s","222.186.52.100","50070");
        configuration.set("fs.defaultFS",uri);
        //UserGroupInformation.createRemoteUser("hdfs");
        FileStatus[] fileStatuses = null;
        try( FileSystem fs = WebHdfsFileSystem.get(configuration);) {
           // webHdfsFileSystem.initialize(new URI("webhdfs://222.186.52.100:50070"),configuration);
           fileStatuses = fs.listStatus(new Path("/"));
        }catch (Exception e) {
            e.printStackTrace();
        }
        return fileStatuses;
    }
}