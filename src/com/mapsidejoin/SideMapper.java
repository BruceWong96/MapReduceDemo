package com.mapsidejoin;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class SideMapper extends Mapper<LongWritable, Text,Text,Item>{
	
	//--用于存储商品表(小表的数据)
	//--key是pid(join的字段),value是对象
	private Map<String,Item> productMap;
	
	/*
	 * 初始化方法，
	 * 目的是读取缓冲文件中的数据（商品表-小表）数据
	 * 然后将商品表数据封装到map中。
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, Text,Item>.Context context)
			throws IOException, InterruptedException {
		
		productMap=new ConcurrentHashMap<String,Item>();
		
		Configuration conf = context.getConfiguration();
		URI[] localCacheFiles = context.getCacheFiles();
		
		//--缓冲的文件的路径存在URI[]的第一个位置
		//--获取分布式文件系统对象
		FileSystem fs = FileSystem.get(localCacheFiles[0], conf);
		
		//--获取缓存文件的输入流
		FSDataInputStream in = fs.open(new Path(localCacheFiles[0]));
		
		BufferedReader br=new BufferedReader(new InputStreamReader(in));
		
		
		String line=null;
		
		while((line=br.readLine())!=null){		
			String[] itemInfo=line.split(" ");
			Item item=new Item();
			item.setPid(itemInfo[0]);
			item.setName(itemInfo[1]);
			item.setPrice(Double.parseDouble(itemInfo[2]));

			productMap.put(item.getPid(),item);
	  
	    }
		br.close();
		
	}
	

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,Text,Item>.Context context)
			throws IOException, InterruptedException { 
		
		String line=value.toString();
		String[] orderInfo=line.split(" ");
		Item item=new Item();
		item.setId(orderInfo[0]);
		item.setDate(orderInfo[1]);
		item.setPid(orderInfo[2]);
		item.setAmount(Integer.parseInt(orderInfo[3]));
		
		//--从map里获取小表的数据，完成join操作
		item.setName(productMap.get(item.getPid()).getName());
		item.setPrice(productMap.get(item.getPid()).getPrice());
		
		
		context.write(new Text(item.getDate()), item);
				
	}
}
