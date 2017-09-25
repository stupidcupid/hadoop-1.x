package com.hadoop.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by nanzhou on 2017/9/19.
 */
public class Sort {

    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable data = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


            String line = value.toString().trim();
            data.set(Integer.parseInt(line));
            context.write(data, new IntWritable(1));
        }

    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {


        private static IntWritable linenum = new IntWritable(1);

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {


            for (IntWritable val : values) {


                context.write(linenum, key);
                linenum = new IntWritable(linenum.get() + 1);

            }
        }
    }


    /**
     * 排序
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String[] ioArgs = new String[]{"/user/hadoop/sort_in", "/user/hadoop/sort_out"};

        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

        if (otherArgs.length != 2) {

            System.err.println("Usage: Data Sort <in> <out>");
            System.exit(2);
        }

        JobConf jobConf = new JobConf();
        jobConf.setJar("/Applications/file/work/JavaProject/hadoopbasic/target/hadoop-basic-1.0-SNAPSHOT.jar");

        Job job = new Job(jobConf, "Data Sort");
        job.setJarByClass(Sort.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}