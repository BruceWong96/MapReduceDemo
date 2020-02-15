package com.combine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(com.combine.WordCountDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// TODO: specify a reducer
		job.setReducerClass(WordCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置Combine组件类
		/**
		 * combine的作用是让合并工作在MapTask提前发生
		 * 这样可以减少reduceTask
		 * 使用combine机制不能改变最后的结果
		 */
		job.setCombinerClass(WordCountCombiner.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.1.130:9000/word"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.130:9000/word/result"));

		if (!job.waitForCompletion(true))
			return;
	}

}
