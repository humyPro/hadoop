package com.humy.mapreduce.temperature;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
		
		List<Integer> list=new LinkedList<>();
		for(IntWritable val:values){
			list.add(val.get());
			
		}

		int max=list.get(0);
		
		for(Integer tmp:list){
			max=Math.max(max, tmp);
			
		}
		context.write(key, new IntWritable(max));
	}
	
}
