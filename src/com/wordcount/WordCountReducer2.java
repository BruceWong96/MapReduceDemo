package com.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 规约聚合
 * 完成单词频数统计
 *  1801	1
 *  hadoop	2
 *  hello	4
 *	world	1
 * @author Ferdinand Wang
 *
 */

public class WordCountReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
		int result = 0;
		
		for (IntWritable value : values) {
			result = result +value.get();
		}
		
		context.write(key, new IntWritable(result));
		
	}
//	@Override
//	protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Text>.Context context)
//			throws IOException, InterruptedException {
//		
//		String result = "";
//		
//		for (IntWritable value : values) {
//			result = result + "," + value.get();
//		}
//		
//		context.write(key, new Text(result));
//		
//	}

}
