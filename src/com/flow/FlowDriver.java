package com.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance();
		job.setJarByClass(com.flow.FlowDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(FlowMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Flow.class);
		
		//设置分区数
		job.setNumReduceTasks(3);
		//设置自定义分区组件
		//如果不设定，默认用的是HashPartitioner
		//默认的分区组件，会按Mapper输出key的hashcode分区，确保相同的key落到相同的一个分区里
		job.setPartitionerClass(FlowPartitioner.class);
		
		// TODO: specify a reducer
		job.setReducerClass(FlowReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Flow.class);
		job.setOutputValueClass(NullWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.1.130:9000/flow"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.130:9000/flow/result"));

		job.waitForCompletion(true);
	}

}
