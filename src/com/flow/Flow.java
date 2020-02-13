package com.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Flow implements Writable{
	
	private String phone;
	private String name;
	private String addr;
	private int flow;
	
	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAddr() {
		return addr;
	}

	public void setAddr(String addr) {
		this.addr = addr;
	}

	public int getFlow() {
		return flow;
	}

	public void setFlow(int flow) {
		this.flow = flow;
	}

	
	
	@Override
	public String toString() {
		return "Flow [phone=" + phone + ", name=" + name + ", addr=" + addr + ", flow=" + flow + "]";
	}

	/**
	 * 序列化方法
	 * Strig 对应 writeUTF()
	 * int 对应writeInt()
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(phone);
		out.writeUTF(name);
		out.writeUTF(addr);
		out.writeInt(flow);
		
	}
	
	/**
	 * 反序列化一定要和序列化的顺序一致
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.phone = in.readUTF();
		this.name = in.readUTF();
		this.addr = in.readUTF();
		this.flow = in.readInt();
	}
	
	
}
