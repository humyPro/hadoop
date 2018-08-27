package com.humy.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountDriver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf=new Configuration();

		Job job=Job.getInstance(conf);
	
		job.setJarByClass(WordCountDriver.class);
		
		job.setMapperClass(WordCountMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(WordCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
	
		job.setOutputValueClass(IntWritable.class);
		
		
		FileInputFormat.setInputPaths(
				job,new Path("hdfs://10.9.60.61:9000/word"));
		
		
		FileOutputFormat.setOutputPath(
				job,new Path("hdfs://10.9.60.61:9000/word/result"));
		
		job.waitForCompletion(true);
	}
}
