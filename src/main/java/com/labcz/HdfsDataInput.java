package com.labcz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.net.URL;

/**
 * @author Labcz
 * @ClassName HdfsDataInput.java
 * @Description 本地文件上传到HDFS,以及从HDFS上将文件下载到本地
 * @createTime 2020年10月30日 19:13:00
 */
public class HdfsDataInput {
    static final Logger logger = Logger.getLogger(HdfsDataInput.class);
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws IOException {
        HdfsDataInput hdi = new HdfsDataInput();
        //将本地桌面上的2.txt文件上传到HDFS的/2.txt
        //hdi.createFile("C:/Users/Labcz/Desktop/1.txt", "/1.txt");
        //将HDFS上的/1.txt文件上获取到本地桌面的1.txt,并且打印1.txt的内容
        hdi.getFile("C:/Users/Labcz/Desktop/hello.txt", "/1.txt",true);
    }

    /**
     * @function 将本地文件上传到HDFS上
     * @param localPath 本地文件路径
     * @param hdfsPath  HDFS目标文件路径
     */
    public void createFile(String localPath,String hdfsPath) {
        //定义输入流
        InputStream in = null;
        try {
        //通用三步骤
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop01:9000");
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsPath), conf);
        //在HDFS上创建空文件
        FSDataOutputStream out = fileSystem.create(new Path(hdfsPath));
        in = new BufferedInputStream(new FileInputStream(new File(localPath)));
        //将文件进行拷贝，从输入流到输出流
        IOUtils.copyBytes(in, out, 4096, false);
        out.hsync();
        out.close();
        logger.info("create file in hdfs:" + hdfsPath);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(in);
        }
    }

    /**
     *@function  从HDFS获取文件，并拷贝到本地localPath
     * @param localPath 本地目标文件路径
     * @param hdfsPath  HDFS文件路径
     * @param print  是否将文件内容同时打印到控制台
     */
    public void getFile(String localPath,String hdfsPath,boolean print) throws IOException {
        //通用三步骤
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop01:9000");
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsPath), conf);
        //定义输入输出流
        FSDataInputStream in = fileSystem.open(new Path(hdfsPath));
        OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(localPath)));
        try{
            //进行文件拷贝
            IOUtils.copyBytes(in,out,4096,!print);
            logger.info("get file form hdfs to local: " + hdfsPath + ", " + localPath);
            if (print) {
                in.seek(0);  //输入流从新回到文件开始的位置
                IOUtils.copyBytes(in, System.out, 4096, true);
            }
            //关闭输出流，否则文件内容可能为空
            out.close();
        }catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(in);
        }
    }
}
