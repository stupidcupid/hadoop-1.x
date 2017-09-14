package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.IOException;


/**
 * Created by nanzhou on 2017/9/13.
 */
public class GetNodeList {


    /**
     * 获取HDFS集群上的所有节点的名称信息
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        DistributedFileSystem hdfs = (DistributedFileSystem) fs;

        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();

        for (int i = 0; i < dataNodeStats.length; i++) {

            System.out.println("DataNode_" + i + "_Name:"
                    + dataNodeStats[i].getHostName());
        }
    }
}
