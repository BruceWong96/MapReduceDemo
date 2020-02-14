package com.zebra;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ZebraReducer extends Reducer<Text, HttpAppHost, HttpAppHost, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<HttpAppHost> values,
			Reducer<Text, HttpAppHost, HttpAppHost, NullWritable>.Context context)
			throws IOException, InterruptedException {

		HttpAppHost resultHttpAppHost = new HttpAppHost();
		
		for (HttpAppHost value : values) {
			resultHttpAppHost.setReportTime(value.getReportTime());
			resultHttpAppHost.setCellid(value.getCellid());
			resultHttpAppHost.setAppType(value.getAppType());
			resultHttpAppHost.setAppSubtype(value.getAppSubtype());
			resultHttpAppHost.setUserIP(value.getUserIP());
			resultHttpAppHost.setUserPort(value.getUserPort());
			resultHttpAppHost.setAppServerIP(value.getAppServerIP());
			resultHttpAppHost.setAppServerPort(value.getAppServerPort());
			
			//累加用户总的接收次数
			resultHttpAppHost.setAccepts(resultHttpAppHost.getAccepts() + value.getAccepts());
			//累加用户总的请求次数
			resultHttpAppHost.setAttempts(resultHttpAppHost.getAttempts() + value.getAttempts());
			//累加用户总的下行流量
			resultHttpAppHost.setTrafficDL(resultHttpAppHost.getTrafficDL() + value.getTrafficDL());
			//累加用户总的上行流量
			resultHttpAppHost.setTrafficUL(resultHttpAppHost.getTrafficUL() + value.getTrafficUL());
			//累加用户总的重传下行流量
			resultHttpAppHost.setRetranDL(resultHttpAppHost.getRetranDL() + value.getRetranUL());
			//累加用户总的重传上行流量
			resultHttpAppHost.setRetranUL(resultHttpAppHost.getRetranUL() + value.getRetranUL());
			//累加总的请求时长
			resultHttpAppHost.setTransDelay(resultHttpAppHost.getTransDelay() + value.getTransDelay());
		}
		context.write(resultHttpAppHost, NullWritable.get());
	}
	

}
