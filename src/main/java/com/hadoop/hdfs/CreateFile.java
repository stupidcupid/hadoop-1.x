package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by nanzhou on 2017/9/14.
 */
public class CreateFile {

    /**
     * 创建HDFS文件
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        byte[] buff = "Hello word hadoop\n".getBytes();

        Path dfs = new Path("/test");

        FSDataOutputStream outputStream = fs.create(dfs);

        outputStream.write(buff, 0, buff.length);

    }
}
