package com.labcz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;

/**
 * @author Labcz
 * @ClassName FileStatusMetadata.java
 * @Description 查看HDFS文件/目录元信息
 * @createTime 2020年10月30日 16:52:00
 */
public class FileStatusMetadata {
    public static void main(String[] args) throws Exception {
        //读取hadoop文件系统的配置
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop01:9000");
        //把HADOOP_USER_NAME设置成系统的全局变量
        System.setProperty("HADOOP_USER_NAME","labcz");
        System.out.println("--------查看HDFS中某文件的元信息--------");
        String fileURI = "/test/hello.txt";
        FileSystem fileSystem = FileSystem.get(URI.create(fileURI),conf);
        //获取文件元信息
        FileStatus fileStatus = fileSystem.getFileStatus(new Path(fileURI));
        if(fileStatus.isDirectory()==false){
            System.out.println("这是个文件");
        }
        System.out.println("文件路径: "+fileStatus.getPath());
        System.out.println("文件长度: "+fileStatus.getLen());
        System.out.println("文件修改日期： "+new Timestamp(fileStatus.getModificationTime()).toString());
        System.out.println("文件上次访问日期： "+new Timestamp(fileStatus.getAccessTime()).toString());
        System.out.println("文件备份数： "+fileStatus.getReplication());
        System.out.println("文件的块大小： "+fileStatus.getBlockSize());
        System.out.println("文件所有者：  "+fileStatus.getOwner());
        System.out.println("文件所在的分组： "+fileStatus.getGroup());
        System.out.println("文件的 权限： "+fileStatus.getPermission().toString());
        System.out.println();

        System.out.println("--------查看HDFS中某文件的元信息--------");
        String dirURI = "/test";
        FileSystem dirFS = FileSystem.get(URI.create(dirURI) ,conf);
        FileStatus dirStatus = dirFS.getFileStatus(new Path(dirURI));
        //获取目录的元信息
        if(dirStatus.isDir()){
            System.out.println("这是个目录");
        }
        System.out.println("目录路径: "+dirStatus.getPath());
        System.out.println("目录长度: "+dirStatus.getLen());
        System.out.println("目录修改日期： "+new Timestamp (dirStatus.getModificationTime()).toString());
        System.out.println("目录上次访问日期： "+new Timestamp(dirStatus.getAccessTime()).toString());
        System.out.println("目录备份数： "+dirStatus.getReplication());
        System.out.println("目录的块大小： "+dirStatus.getBlockSize());

        System.out.println("目录所有者：  "+dirStatus.getOwner());
        System.out.println("目录所在的分组： "+dirStatus.getGroup());
        System.out.println("目录的权限： "+dirStatus.getPermission().toString());
        System.out.println("这个目录下包含以下文件或目录：");
        FileStatus[] fsList = dirFS.listStatus(new Path(dirURI));
        for(FileStatus fs : fsList){
            System.out.println(fs.getPath());
        }
    }
}
