package com.hadoop.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 * Created by nanzhou on 2017/9/14.
 */
public class Dedup {



    //map将输入中的value复制到输出数据的key上，并直接输出
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private static Text line = new Text();//每行数据

        //实现map函数
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            line = value;
            context.write(line, new Text(""));
            System.out.println(line);
        }
    }

    //reduce将输入中的key复制到输出数据的key上，并直接输出
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        //实现reduce函数
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, new Text(""));
            System.out.println(key);
        }
    }

    /**
     * 数据去重
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();


        String[] ioArgs = new String[]{"/user/hadoop/dedup_in", "/user/hadoop/dedup_out"};

        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

        if (otherArgs.length != 2) {

            System.err.println("Usage: Data Dedupcation <in> <out>");
            System.exit(2);
        }

        JobConf jobConf = new JobConf();
        jobConf.setJar("/Applications/file/work/JavaProject/hadoopbasic/target/hadoop-basic-1.0-SNAPSHOT.jar");

        // /Applications/file/work/JavaProject/hadoopbasic/target/hadoop-basic-1.0-SNAPSHOT.jar
        Job job = new Job(jobConf, "Data Dedupcation");
        job.setJarByClass(Dedup.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
