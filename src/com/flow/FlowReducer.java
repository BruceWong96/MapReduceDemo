package com.flow;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<Text, Flow, Flow, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<Flow> values, Reducer<Text, Flow, Flow, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		Flow resultFlow = new Flow();
		
		for (Flow value : values) {
			resultFlow.setPhone(value.getPhone());
			resultFlow.setName(value.getName());
			resultFlow.setAddr(value.getAddr());
			
			resultFlow.setFlow(resultFlow.getFlow() + value.getFlow());
		}
		context.write(resultFlow, NullWritable.get());
	}

}
