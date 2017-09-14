package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by nanzhou on 2017/9/13.
 */
public class ListAllFile {

    /**
     * core-defult.xml core-size.xml mapred-defult.xml mapred-site.xml
     * 读取HDFS文件目录下的全部文件
     *
     * @param args
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();

        FileSystem hdfs = FileSystem.get(conf);
        Path listf = new Path("/user/hadoop/input");
        FileStatus stats[] = hdfs.listStatus(listf);
        for (int i = 0; i < stats.length; ++i) {
            System.out.println(stats[i].getPath().toString() + " " + stats[i].getPermission());
        }
        hdfs.close();
    }
}
