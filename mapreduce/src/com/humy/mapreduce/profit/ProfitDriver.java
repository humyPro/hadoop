package com.humy.mapreduce.profit;

import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ProfitDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(ProfitDriver.class);
		job.setMapperClass(ProfitMapper.class);
		job.setReducerClass(ProfitReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(
				job,new Path("hdfs://10.9.60.61:9000/profit"));
		
		
		FileOutputFormat.setOutputPath(
				job,new Path("hdfs://10.9.60.61:9000/profit/result"));
		
		job.waitForCompletion(true);
		if(job.waitForCompletion(true)){
			Job job2=Job.getInstance(conf);
			job2.setJarByClass(ProfitDriver.class);
			job2.setMapperClass(SortProfitMapper.class);
			job2.setMapOutputKeyClass(Profit.class);
			job2.setMapOutputValueClass(NullWritable.class);
			
			FileInputFormat.setInputPaths(
					job2,new Path("hdfs://10.9.60.61:9000/profit/result"));
			
			
			FileOutputFormat.setOutputPath(
					job2,new Path("hdfs://10.9.60.61:9000/profit/result/sort"));
			
			job2.waitForCompletion(true);
		}
	}
}
