package com.humy.mapreduce.profit;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortProfitMapper extends Mapper<LongWritable, Text, Profit, NullWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line=ivalue.toString();
		String[] vals=line.split("\\s+");
		Profit p=new Profit();
		p.setName(vals[0]);
		p.setProfit(Integer.parseInt(vals[1]));
		context.write(p, NullWritable.get());
	}

}
