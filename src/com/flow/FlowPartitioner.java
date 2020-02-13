package com.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区器
 * 第一个泛型类型是map的输出key类型
 * 第二个泛型类型是map的输出value类型
 * 分区编号从0开始
 * @author wenjie
 *
 */
public class FlowPartitioner extends Partitioner<Text, Flow>{

	@Override
	public int getPartition(Text key, Flow value, int numPartitions) {
		
		if (value.getAddr().equals("bj")) {
			return 0;
		}
		else if (value.getAddr().equals("sh")) {
			return 1;
		}
		else {
			return 2;
		}
	}
	
}
