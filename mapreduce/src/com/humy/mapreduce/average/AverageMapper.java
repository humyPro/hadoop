package com.humy.mapreduce.average;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line=ivalue.toString();
		String[] nameAndScore=line.split(" ");
		context.write(new Text(nameAndScore[0]), 
				new IntWritable(Integer.parseInt(nameAndScore[1])));
	}

}
