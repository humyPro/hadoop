package com.humy.mapreduce.average;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AverageDriver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf=new Configuration();

		Job job=Job.getInstance(conf);
	
		job.setJarByClass(AverageDriver.class);
		
		job.setMapperClass(AverageMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(AverageReducer.class);
		
		job.setOutputKeyClass(Text.class);
	
		job.setOutputValueClass(IntWritable.class);
		
		
		FileInputFormat.setInputPaths(
				job,new Path("hdfs://10.9.60.61:9000/average"));
		
		
		FileOutputFormat.setOutputPath(
				job,new Path("hdfs://10.9.60.61:9000/average/result"));
		
		job.waitForCompletion(true);
	}
}
