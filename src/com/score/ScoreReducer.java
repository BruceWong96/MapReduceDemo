package com.score;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreReducer extends Reducer<Text, Student, Student, NullWritable>{
	@Override
	protected void reduce(Text key, Iterable<Student> values, Reducer<Text, Student, Student, NullWritable>.Context context)
			throws IOException, InterruptedException {
		Student resultStudent = new Student();
		
		resultStudent.setName(key.toString());
		
		for(Student student : values){
			resultStudent.setChinese(resultStudent.getChinese() + student.getChinese());
			resultStudent.setEnglish(resultStudent.getEnglish() + student.getEnglish());
			resultStudent.setMath(resultStudent.getMath() + student.getMath());
		}
		
		context.write(resultStudent, NullWritable.get());
	}
}
