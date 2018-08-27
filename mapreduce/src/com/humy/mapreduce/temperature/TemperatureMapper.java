package com.humy.mapreduce.temperature;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		String year=line.substring(8, 12);
		String temp=line.substring(line.length()-4);
		context.write(new Text(year),
				new IntWritable(Integer.parseInt(temp)));
	}
	

}
