package com.labcz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * @author Labcz
 * @ClassName FileCopyWithProgress.java
 * @Description  文件上传显示进度条
 * @createTime 2020年10月30日 21:58:00
 */
public class FileCopyWithProgress {
    public static void main(String[] args) throws Exception {
        String localURI = "C:/Users/Labcz/Desktop/1.jpg";
        String destURI = "/1.jpg";
        InputStream in =new BufferedInputStream(new FileInputStream(localURI));
        //通用三步骤
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop01:9000");
        FileSystem fileSystem = FileSystem.get(URI.create(destURI), conf);
        //在输出流中定义进度条
        OutputStream out = fileSystem.create(new Path(destURI), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, out, 4096, true);
    }
}
