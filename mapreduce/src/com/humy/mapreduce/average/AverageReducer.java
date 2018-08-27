package com.humy.mapreduce.average;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	static int i=0;
	public void reduce(Text _key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int total=0;
		int count=0;
		for (IntWritable val : values) {
			total=total+val.get();
			count++;
			i++;
			System.out.println("reduce中的value:i="+i+","+val);
		}
		context.write(_key, new IntWritable(total/count));
		i=0;
	}

}
