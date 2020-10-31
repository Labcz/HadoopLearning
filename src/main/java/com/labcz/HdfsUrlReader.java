package com.labcz;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * @author Labcz
 * @ClassName HdfsUrlReader.java
 * @Description 根据URL读取文件内容
 * @createTime 2020年10月30日 15:54:00
 */
public class HdfsUrlReader {
    static {
        //通过Java的URL类来访问Hdfs内的文件
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }
    public static void main(String[] args) {
        //定义一个JAVA的输入流
        InputStream inputStream = null;
        try {
            //读取HDFS上的1.txt文件（提前上传到HDFS的/目录下）192.168.229.131为HDFS文件系统地址
            inputStream = new URL("hdfs://hadoop01:9000/1.txt").openStream();
            //获取输入流，输出到控制台，每次拷贝缓冲区buffer大小，最后是否关闭数据流
            IOUtils.copyBytes(inputStream,System.out,1024,true);
            System.out.print("打印完成！");
        } catch (IOException e) {
            //若出现异常，关闭数据流
            IOUtils.closeStream(inputStream);
            e.printStackTrace();
        }

    }
}
