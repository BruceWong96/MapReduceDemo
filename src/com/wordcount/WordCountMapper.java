package com.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Ferdinand Wang
 * 1.job的MapTask如何处理文件块数据,是由Mapper组件类来决定，此类里的代码需要开发者来写
 * 2.开发一个Mapper组件的方式是让一个类继承Mapper
 * 3.第一个泛型类型对应的是MapTask的输入Key类型
 * 4.第二个泛型类型对应的是MapTask的输入Value类型
 * 5.输入key是每行的行首偏移量，所以是LongWritable
 * 6.输入value是每行的内容，所以是Text
 * 7.Writable机制是Hadoop的序列化机制
 * 常用类型LongWritable，Text，NullWritable
 * 8.map()方法用于将输入key和输入value传给程序员，一行数据调用一次
 * 9.第三个泛型类型是MapTask的输出key类型
 * 10.第四个泛型类型是MapTask的输出value类型
 * 11.第一个和第二个泛型类型写死，第三个和第四个类型不固定
 * 
 */
public class WordCountMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		//将输入key和输入value做输出
		context.write(key, value);
	}
	
}
