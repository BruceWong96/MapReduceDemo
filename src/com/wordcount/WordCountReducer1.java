package com.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 规约聚合
 * 
 * 1.第一个泛型类型对应的Mapper的输出key类型
 * 2.第二个泛型类型对应的Mapper的输出value类型
 * 3.第三个泛型类型对应的reduce的输出key类型
 * 4.第四个泛型类型对应的reduce的输出value类型
 * 5.Reduce组件不能单独存在，因为要接受Mapper组件的输出
 * 6.Mapper组件可以单独存在，当只有Mapper时，最后的结果文件是MapTask的输出
 * 7.当既有Mapper又有Reduce时，最后的记过文件时Reduce输出，而Mapper的输出作为中间结果
 * 
 * @author Ferdinand Wang
 *
 */

public class WordCountReducer1 extends Reducer<Text, IntWritable, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String result = "";
		
		for (IntWritable value : values) {
			result = result + "," + value.get();
		}
		
		context.write(key, new Text(result));
		
	}

}
