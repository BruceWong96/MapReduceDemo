package com.profit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProfitDriver2 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(com.profit.ProfitDriver2.class);
		// TODO: specify a mapper
		job.setMapperClass(ProfitMapper2.class);
		// TODO: specify a reducer
		job.setMapOutputKeyClass(Profit.class);
		job.setMapOutputValueClass(NullWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.1.130:9000/profit/result"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.130:9000/profit/result2"));

		if (!job.waitForCompletion(true))
			return;
	}

}
