package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by nanzhou on 2017/9/14.
 */
public class GetLTime {

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException {


        Configuration conf = new Configuration();

        FileSystem hdfs = FileSystem.get(conf);

        Path path = new Path("/user/hadoop/input/file1.txt");

        FileStatus fileStatus = hdfs.getFileStatus(path);

        System.out.println("file last modify time is " + fileStatus.getModificationTime());
    }
}
