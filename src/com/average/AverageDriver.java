package com.average;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 输出结果为
 * jary	73.75
 * rose	117.66666666666667
 *  tom	73.66666666666667
 * @author Ferdinand Wang
 *
 */

public class AverageDriver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance();
		
		job.setJarByClass(com.average.AverageDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(AverageMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// TODO: specify a reducer
		job.setReducerClass(AverageReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.1.130:9000/average"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.130:9000/average/result"));

		job.waitForCompletion(true);
	
	}

}
