package com.humy.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		//--完成单词的频次统计
		//--用到的知识点：IntWritable->Int: IntWritable.get()
		//--Int->IntWritable:new IntWritable(Int)
		
		int count=0;
		for(IntWritable value:values){
			//--第一种方式：count++;
			count=count+value.get();
		}
		context.write(key,new IntWritable(count));
	}

}
