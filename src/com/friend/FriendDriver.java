package com.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FriendDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(com.friend.FriendDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(FriendMapper1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(FriendReducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.1.130:9000/friend"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.130:9000/friend/result"));

		if (job.waitForCompletion(true)){
			Job job2 = Job.getInstance(conf);
			
			// TODO: specify a mapper
			job2.setMapperClass(FriendMapper2.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);
			
			job2.setReducerClass(FriendReducer2.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(NullWritable.class);

			// TODO: specify input and output DIRECTORIES (not files)
			FileInputFormat.setInputPaths(job2, new Path("hdfs://192.168.1.130:9000/friend/result"));
			FileOutputFormat.setOutputPath(job2, new Path("hdfs://192.168.1.130:9000/friend/result2"));
			job2.waitForCompletion(true);
		}
			
	}

}
