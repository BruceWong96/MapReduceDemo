package com.movie;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * MR框架会按照Mapper的输出key排序，具体排序的规则是由输出key的类型来决定的
 * 实际上是由输出key类型的compareTo()来决定的。
 * 注意：MR框架只能按照Mapper的输出key来进行排序
 * @author wenjie
 *
 */
public class Movie implements WritableComparable<Movie>{

	private String name;
	private int hot;
	
	public String getname() {
		return name;
	}

	public void setname(String name) {
		this.name = name;
	}

	public int getHot() {
		return hot;
	}

	public void setHot(int hot) {
		this.hot = hot;
	}

	@Override
	public String toString() {
		return "Movie [name=" + name + ", hot=" + hot + "]";
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(name);
		out.writeInt(hot);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.name = in.readUTF();
		this.hot = in.readInt();
	}

	
	@Override
	public int compareTo(Movie movie) {
		return movie.hot - this.hot;
	}

}
