package com.max;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		
		Job job = Job.getInstance();
		
		job.setMapperClass(MaxMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(MaxReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		
		FileInputFormat.setInputPaths(job, 
				new org.apache.hadoop.fs.Path("hdfs://192.168.1.130:9000/max"));
		
		//输出结果路径要求 是一个不存在的目录
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.130:9000/max/result"));
		
		job.waitForCompletion(true);
		
		
	}

}
