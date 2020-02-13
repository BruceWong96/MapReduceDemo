package com.flow;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, Text, Flow> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Flow>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		Flow flow = new Flow();
		String [] info = line.split(" ");
		
		flow.setPhone(info[0]);
		flow.setName(info[1]);
		flow.setAddr(info[2]);
		flow.setFlow(Integer.parseInt(info[3]));

		context.write(new Text(flow.getName()), flow);
		
	}
}
