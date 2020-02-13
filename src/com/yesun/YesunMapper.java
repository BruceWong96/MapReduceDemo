package com.yesun;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YesunMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] people = line.split(" ");
		
		context.write(new Text(people[0]), new Text("+"+people[1]));
		context.write(new Text(people[1]), new Text("-"+people[0]));
		
	}

}
