package com.humy.mapreduce.totalsort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalSortMapper extends Mapper<LongWritable, Text, IntWritable,IntWritable > {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		String[] values=line.split(" ");
		for(String val:values){
			context.write(new IntWritable(Integer.parseInt(val)), new IntWritable(1));
		}
		
	}
	
	

}
