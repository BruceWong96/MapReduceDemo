package com.average;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
		
		//平均值
		Double average = 0.0;
		
		//求和
		Double sum = 0.0;
		
		//计数器
		Integer count = 0;
		
		for (IntWritable value : values) {
			sum += value.get();
			count++;
		}
		average = sum/count;
		
		context.write(key, new DoubleWritable(average));
		
		
		
	}
	

}
