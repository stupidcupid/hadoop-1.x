package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by nanzhou on 2017/9/14.
 */
public class CopyFile {


    /**
     * 上传本地文件
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {


        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        Path src = new Path("/Applications/file/eclipse-jee-indigo-SR2-macosx-cocoa-x86_64.tar.gz");

        Path dfs = new Path("/");

        System.out.println("Upload to" + conf.get("fs.default.name"));
        fs.copyFromLocalFile(src, dfs);

        FileStatus files[] = fs.listStatus(dfs);

        for (FileStatus file : files) {

            System.out.println(file.getPath());
        }
    }
}
