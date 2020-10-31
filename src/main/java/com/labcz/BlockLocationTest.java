package com.labcz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @author Labcz
 * @ClassName BlockLocation.java
 * @Description 查找某个文件在HDFS集群的位置
 * @createTime 2020年10月30日 17:40:00
 */
public class BlockLocationTest {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop01:9000");
        String fileURI = "/test/hello.txt";
        FileSystem fileSystem = FileSystem.get(URI.create(fileURI),conf);
        //获取文件元信息
        FileStatus fileStatus = fileSystem.getFileStatus(new Path(fileURI));
        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        int blockLen = blockLocations.length;
        for(int i=0;i<blockLen;i++){
            String[] hosts = blockLocations[i].getHosts();
            //其中hosts[0]离自己最近
            System.out.println("block_"+i+"_location hosts[0]:"+hosts[0]);
            System.out.println("block_"+i+"_location hosts[1]:"+hosts[1]);
        }

    }
}
