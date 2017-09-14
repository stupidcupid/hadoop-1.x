package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by nanzhou on 2017/9/14.
 */
public class CheckFileExits {

    /**
     * 检查HDFS文件是否存在
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();

        FileSystem hdfs = FileSystem.get(conf);

        Path path = new Path("/NewTestDir");

        boolean flag = hdfs.exists(path);

        System.out.println("is Exits " + flag);

    }
}
