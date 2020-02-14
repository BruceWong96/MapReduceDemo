package com.zebra;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ZebraMapper extends Mapper<LongWritable, Text, Text, HttpAppHost> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, HttpAppHost>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String [] data = line.split("\\|");
		
		HttpAppHost httpAppHost = new HttpAppHost();
		
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		//获取用户上网日期
		String reportTime = fileSplit.getPath().getName().split("_")[1];
		httpAppHost.setReportTime(reportTime);
		
		//小区id
		httpAppHost.setCellid(data[16]);
		//应用大类
		httpAppHost.setAppType(Integer.parseInt(data[22]));
		//应用子类
		httpAppHost.setAppSubtype(Integer.parseInt(data[23]));
		//用户上网ip
		httpAppHost.setUserIP(data[26]);
		//用户上网端口
		httpAppHost.setUserPort(Integer.parseInt(data[28]));
		//服务端ip
		httpAppHost.setAppServerIP(data[30]);
		//服务端口号
		httpAppHost.setAppServerPort(Integer.parseInt(data[32]));
		//域名
		httpAppHost.setHost(data[58]);
		
		//获取请求响应码，此业务中只关心是否=103，如果返回103，表示是一次成功的http请求
		int appTypeCode=Integer.parseInt(data[18]);
		//http的状态码
		String transStatus=data[54];
		 
		if(httpAppHost.getCellid()==null||httpAppHost.getCellid().equals("")){
		httpAppHost.setCellid("000000000");
		}
		//设置请求次数为1
		if(appTypeCode==103){
		httpAppHost.setAttempts(1);
		}
		//设置接收次数为1
		if(appTypeCode==103&&"10,11,12,13,14,15,32,33,34,35,36,37,38,48,49,50,51,52,53,54,55,199,200,201,202,203,204,205,206,302,304,306".contains(transStatus)){
		httpAppHost.setAccepts(1);
		}else{
		httpAppHost.setAccepts(0);
		}
		
		//上行流量
		if(appTypeCode == 103){
		httpAppHost.setTrafficUL(Long.parseLong(data[33]));
		}
		 
		//下行流量
		if(appTypeCode == 103){
		httpAppHost.setTrafficDL(Long.parseLong(data[34]));
		}
		 
		//重传上行流量
		if(appTypeCode == 103){
		httpAppHost.setRetranUL(Long.parseLong(data[39]));
		}
		 
		//重传下行流量
		if(appTypeCode == 103){
		httpAppHost.setRetranDL(Long.parseLong(data[40]));
		}
		 
		//设置请求时间
		if(appTypeCode==103){
		httpAppHost.setTransDelay(Long.parseLong(data[20]) - Long.parseLong(data[19]));
		}

		String userKey=httpAppHost.getReportTime() + "|" 
				+ httpAppHost.getAppType() + "|" 
				+ httpAppHost.getAppSubtype() + "|" 
				+ httpAppHost.getUserIP() + "|" 
				+ httpAppHost.getUserPort() + "|" 
				+ httpAppHost.getAppServerIP() + "|" 
				+ httpAppHost.getAppServerPort() +"|" 
				+ httpAppHost.getHost() + "|" 
				+ httpAppHost.getCellid();
		
		context.write(new Text(userKey), httpAppHost);
		
	}

}
