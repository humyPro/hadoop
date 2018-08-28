package com.humy.mapreduce.profit;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfitMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line=ivalue.toString();
		String[] vals=line.split(" +");

		IntWritable prifit=new IntWritable(Integer.parseInt(vals[2])-Integer.parseInt(vals[3]));
		context.write(new Text(vals[1]),prifit);
	}

}
