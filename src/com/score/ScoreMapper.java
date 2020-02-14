package com.score;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ScoreMapper extends Mapper<LongWritable, Text, Text, Student>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Student>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		String name = line.split(" ")[1];
		int score = Integer.parseInt(line.split(" ")[2]);
		
		Student student = new Student();
		
		student.setName(name);
		
		//可以通过获取当前MapTask处理的切片信息来获取文件名
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		//获取文件名
		String fileName = fileSplit.getPath().getName();
		
		if (fileName.contains("chinese")) {
			student.setChinese(score);
		}else if (fileName.contains("english")) {
			student.setEnglish(score);
		}else {
			student.setMath(score);
		}
		
		context.write(new Text(name), student);
		
	}
}
