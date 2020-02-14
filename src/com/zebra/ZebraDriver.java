package com.zebra;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ZebraDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance();
		job.setJarByClass(com.zebra.ZebraDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(ZebraMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(HttpAppHost.class);
		
		// TODO: specify a reducer
		job.setReducerClass(ZebraReducer.class);

		job.setOutputKeyClass(HttpAppHost.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.1.130:9000/zebra"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.130:9000/zebra/result"));

		job.waitForCompletion(true);
	}

}
