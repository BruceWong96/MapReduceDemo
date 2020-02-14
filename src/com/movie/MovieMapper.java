package com.movie;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper extends Mapper<LongWritable, Text, Movie, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Movie, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		Movie movie = new Movie();
		movie.setname(line.split(" ")[0]);
		movie.setHot(Integer.parseInt(line.split(" ")[1]));
		
		context.write(movie, NullWritable.get());
	}

}
