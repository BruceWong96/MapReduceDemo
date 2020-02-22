package com.mapsidejoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SideDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(SideDriver.class);
		job.setMapperClass(SideMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Item.class);
		
		//--将指定路径的文件数据，缓冲到MapTask中，
		//--这样一来，每一个MapTask运行是，缓冲中都有这个数据了
		job.addCacheFile(new Path("hdfs://192.168.1.130:9000/cachejoin/product.txt").toUri());
		
		
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.130:9000/join"));
		FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.1.130:9000/join/result"));
		
		job.waitForCompletion(true);
	}
}
