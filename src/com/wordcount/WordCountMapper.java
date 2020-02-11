package com.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Ferdinand Wang
 * 1.job的MapTask如何处理文件块数据
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
	
}
