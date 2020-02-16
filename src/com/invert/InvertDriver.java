package com.invert;

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

import com.profit.Profit;
import com.profit.ProfitMapper1;
import com.profit.ProfitMapper2;
import com.profit.ProfitReducer1;

public class InvertDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(InvertDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(InvertMapper1.class);
		// TODO: specify a reducer
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(InvertReducer1.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.1.130:9000/invert"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.130:9000/invert/result"));

		if (job.waitForCompletion(true)){
			//执行job2
			Job job2 = Job.getInstance(conf);
			
			job2.setMapperClass(InvertMapper2.class);
			// TODO: specify a reducer
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			
			job2.setReducerClass(InvertReducer2.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			// TODO: specify input and output DIRECTORIES (not files)
			FileInputFormat.setInputPaths(job2, new Path("hdfs://192.168.1.130:9000/invert/result"));
			FileOutputFormat.setOutputPath(job2, new Path("hdfs://192.168.1.130:9000/invert/result2"));
			
			job2.waitForCompletion(true);
		}
			
	}

}
