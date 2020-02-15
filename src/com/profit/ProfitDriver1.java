package com.profit;

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

public class ProfitDriver1 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(com.profit.ProfitDriver1.class);
		// TODO: specify a mapper
		job.setMapperClass(ProfitMapper1.class);
		// TODO: specify a reducer
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(ProfitReducer1.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.1.130:9000/profit"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.130:9000/profit/result"));

		if (job.waitForCompletion(true)){
			//执行job2
			Job job2 = Job.getInstance(conf);
			
			job2.setMapperClass(ProfitMapper2.class);
			// TODO: specify a reducer
			job2.setMapOutputKeyClass(Profit.class);
			job2.setMapOutputValueClass(NullWritable.class);

			// TODO: specify input and output DIRECTORIES (not files)
			FileInputFormat.setInputPaths(job2, new Path("hdfs://192.168.1.130:9000/profit/result"));
			FileOutputFormat.setOutputPath(job2, new Path("hdfs://192.168.1.130:9000/profit/result2"));
			
			job2.waitForCompletion(true);
		}
			
	}

}
