package com.humy.mapreduce.profit;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfitReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text _key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int totalPrifit=0;
		for (IntWritable val : values) {
			totalPrifit=totalPrifit+val.get();
		}
		context.write(_key, new IntWritable(totalPrifit));
	}

}
