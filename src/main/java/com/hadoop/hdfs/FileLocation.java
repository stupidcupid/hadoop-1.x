package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by nanzhou on 2017/9/14.
 */
public class FileLocation {


    /**
     * 查找某个文件在HDFS集群的位置
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();

        FileSystem hdfs = FileSystem.get(conf);

        Path path = new Path("/eclipse-jee-indigo-SR2-macosx-cocoa-x86_64.tar.gz");

        FileStatus status = hdfs.getFileStatus(path);

        BlockLocation blocks[] = hdfs.getFileBlockLocations(status, 0, status.getLen());

        for (int i = 0; i < blocks.length; i++) {

            System.out.println("block_" + i + blocks[i].getHosts()[0]);

        }

    }
}

