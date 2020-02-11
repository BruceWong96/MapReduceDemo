package com.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		//获取job对象
		Job job = Job.getInstance();
		
		//设置job方法入口的驱动类
		job.setJarByClass(WordCountDriver.class);
		
		//设置Mapper组件类
		job.setMapperClass(WordCountMapper1.class);
		
		//设置Mapper的输出key和value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//设置reducer组件类
		job.setReducerClass(WordCountReducer2.class);
		
		//设置reducer的key和value输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, 
				new org.apache.hadoop.fs.Path("hdfs://192.168.1.130:9000/word"));
		
		//输出结果路径要求 是一个不存在的目录
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.130:9000/word/result"));
		
		//提交job
		job.waitForCompletion(true);
	}

}
