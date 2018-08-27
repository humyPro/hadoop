package com.humy.mapreduce.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper_1 extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		//做text和String的转换
		String line=ivalue.toString();
		String[] words=line.split(" ");
		for(String word:words){
			context.write(new Text(word), new IntWritable(1));
		}
	}

}
