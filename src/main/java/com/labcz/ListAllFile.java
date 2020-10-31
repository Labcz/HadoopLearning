package com.labcz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @author Labcz
 * @ClassName ListAllFile.java
 * @Description 读取HDFS某个目录下的所有文件
 * @createTime 2020年10月30日 23:07:00
 */
public class ListAllFile {
    public static void main(String[] args) throws IOException {
        //目标文件夹路径
        String uri = "/test";
        //通用三步骤
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop01:9000");
        FileSystem fileSystem = FileSystem.get(URI.create(uri), conf);

        Path[] paths = new Path[1];
        paths[0] = new Path(uri);
        FileStatus[] status = fileSystem.listStatus(paths);
        for (FileStatus p : status) {
            System.out.println(p);
        }
        Path[] listedPaths = FileUtil.stat2Paths(status);
        for (Path p : listedPaths) {
            System.out.println(p);
        }
    }
}
