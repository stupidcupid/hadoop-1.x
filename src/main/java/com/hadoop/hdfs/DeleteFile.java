package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by nanzhou on 2017/9/13.
 */
public class DeleteFile {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        FileSystem hdfs = FileSystem.get(conf);

        Path delFile = new Path("/user/hadoop/output1");

        boolean isDeleted = hdfs.delete(delFile);

        System.out.println("Deleted ? " + isDeleted);
    }
}
