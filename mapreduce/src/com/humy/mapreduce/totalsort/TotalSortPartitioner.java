package com.humy.mapreduce.totalsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TotalSortPartitioner extends Partitioner<IntWritable, IntWritable>{

	@Override
	public int getPartition(IntWritable key, IntWritable value, int num) {
		int val=key.get();
		if(val<=100)
			return 0;
		else if(val<1000)
			return 1;
		return 2;
	}

}
