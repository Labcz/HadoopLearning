package com.labcz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * @author Labcz
 * @ClassName FileSystemReader.java
 * @Description 使用FileSystem类访问HDFS
 * @createTime 2020年10月30日 16:21:00
 */
public class FileSystemReader {
    public static void main(String[] args) throws Exception {
        // 会默认读取core-site.xml中的fs.default.name来判断文件系统。如果未设置则默认是本地文件系统
        Configuration configuration = new Configuration();
        //定义访问路径
        String uri = "hdfs://hadoop01:9000/1.txt";
        //定义FileSystem
        FileSystem fileSystem = FileSystem.get(URI.create(uri), configuration);
        //定义输入流
        InputStream inputStream = null;
        try {
            //将String类型的url地址转换为Path类型，也可以直接定义Path类型路径
            inputStream = fileSystem.open(new Path(uri));
            //获取输入流，输出到控制台，每次拷贝缓冲区buffer大小，最后不关闭数据流
            IOUtils.copyBytes(inputStream, System.out, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            //关闭数据流
            IOUtils.closeStream(inputStream);
        }

    }
}
