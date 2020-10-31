package com.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;


/**
 * @author Labcz
 * @ClassName HdfsJavaApi.java
 * @Description 对文件上传下载、修改、删除、查看等等操作
 * @createTime 2020年10月30日 23:14:00
 */
public class HdfsJavaApi {
    /**
     * @function 从HDFS上获取文件到本地
     * @param fileSystem    传入DFS文件系统
     * @param hdfsPathStr   HDFS文件路径
     * @param localPathStr   本地文件路径
     */
    public void getFile(FileSystem fileSystem,String hdfsPathStr,String localPathStr) throws IOException {
        //定义输入输出流
        FSDataInputStream in = fileSystem.open(new Path(hdfsPathStr));
        OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(localPathStr)));
        try{
            //进行文件拷贝
            IOUtils.copyBytes(in,out,4096,false);
            in.seek(0);  //输入流从新回到文件开始的位置
            IOUtils.copyBytes(in, System.out, 4096, true);
            //关闭输出流，否则文件内容可能为空
            out.close();
            System.out.println("GET SUCCESSFULLY");
        }catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(in);
        }
    }

    /**
     * @function 将本地文件上传到文件系统
     * @param fileSystem    传入DFS文件系统
     * @param hdfsPathStr   HDFS文件路径
     * @param localPathStr  本地文件路径
     */
    public void putFile(FileSystem fileSystem,String hdfsPathStr,String localPathStr) throws Exception {
        //在HDFS上创建空文件
        FSDataOutputStream out = fileSystem.create(new Path(hdfsPathStr));
        InputStream in = new BufferedInputStream(new FileInputStream(new File(localPathStr)));
        try {
            //将文件进行拷贝，从输入流到输出流
            IOUtils.copyBytes(in, out, 4096, false);
            out.close();
            System.out.println("PUT SUCCESSFULLY");
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(in);
        }
    }

    /**
     * @function 删除文件
     * @param fileSystem    传入DFS文件系统
     * @param hdfsPathStr   HDFS文件路径
     */
    public void delFile(FileSystem fileSystem,String hdfsPathStr,boolean recursive){
        try {
            //true：开启级联删除，将删除目录下的全部内容
            fileSystem.delete(new Path(hdfsPathStr),recursive);
            System.out.println("DELETE SUCCESSFULLY");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @function 列出路径下全部文件内容
     * @param fileSystem    传入DFS文件系统
     * @param hdfsPathStr   HDFS目录
     */
    public void listFile(FileSystem fileSystem,String hdfsPathStr) throws IOException {
        Path[] paths = new Path[1];
        paths[0] = new Path(hdfsPathStr);
        FileStatus[] status = fileSystem.listStatus(paths);
        for (FileStatus p : status) {
            System.out.println(p);
        }
        Path[] listedPaths = FileUtil.stat2Paths(status);
        for (Path p : listedPaths) {
            System.out.println(p);
        }
    }

    public static void main(String[] args) throws Exception {
        String cmd = args[0];
        String hdfsPathStr = args[1];
        //通用三步骤
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop01:9000");
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsPathStr), conf);
        HdfsJavaApi hdfsJavaApi =new HdfsJavaApi();
        if(cmd.equals("get")){
            hdfsJavaApi.getFile(fileSystem,hdfsPathStr,args[2]);
        }else if(cmd.equals("put")){
            hdfsJavaApi.putFile(fileSystem,hdfsPathStr,args[2]);
        }else if(cmd.equals("delete")){
            hdfsJavaApi.delFile(fileSystem,hdfsPathStr,Boolean.parseBoolean(args[2]));
        }else if(cmd.equals("list")){
            hdfsJavaApi.listFile(fileSystem,hdfsPathStr);
        }else{
            System.out.println("HDFS JAVA API ERROR:COMMAND NOT FOUND!");
        }

    }
}
