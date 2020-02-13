package com.yesun;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class YesunDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance();		
		job.setJarByClass(com.yesun.YesunDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(YesunMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// TODO: specify a reducer
		job.setReducerClass(YeSunReducer.class);
		
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.1.130:9000/yesun"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.130:9000/yesun/result"));

		job.waitForCompletion(true);
		
	}

}
