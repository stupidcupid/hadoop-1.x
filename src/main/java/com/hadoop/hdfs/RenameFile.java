package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by nanzhou on 2017/9/14.
 */
public class RenameFile {


    /**
     * 重命名HDFS文件
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();

        FileSystem hdfs = FileSystem.get(conf);

        Path frPath = new Path("/TestDir");
        Path toPath = new Path("/NewTestDir");

        boolean flag = hdfs.rename(frPath, toPath);

        System.out.println("Renamed ? " + flag);

    }
}
