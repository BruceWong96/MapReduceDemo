package com.totalsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotalSortDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(com.totalsort.TotalSortDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(TotalSortMapper.class);
		// TODO: specify a reducer
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(TotalSortReducer.class);
		// TODO: specify output types
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setPartitionerClass(TotalSortPartition.class);
		job.setNumReduceTasks(3);
		
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.1.130:9000/totalsort"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.130:9000/totalsort/result"));

		job.waitForCompletion(true);
	}

}
