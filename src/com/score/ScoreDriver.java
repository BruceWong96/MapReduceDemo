package com.score;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ScoreDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		
		Job job = Job.getInstance();
		
		job.setJarByClass(ScoreDriver.class);
		
		job.setMapperClass(ScoreMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Student.class);
		job.setReducerClass(ScoreReducer.class);
		job.setOutputKeyClass(Student.class);
		job.setOutputValueClass(NullWritable.class);

		
		FileInputFormat.setInputPaths(job, 
				new org.apache.hadoop.fs.Path("hdfs://192.168.1.130:9000/score"));
		
		//输出结果路径要求 是一个不存在的目录
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.130:9000/score/result"));
		
		job.waitForCompletion(true);
		
		
	}
}
