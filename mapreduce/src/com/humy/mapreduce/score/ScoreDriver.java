package com.humy.mapreduce.score;

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


public class ScoreDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(ScoreDriver.class);
		job.setMapperClass(SocreMapper.class);
		job.setReducerClass(ScoreReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Score.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Score.class);
		
		FileInputFormat.setInputPaths(
				job,new Path("hdfs://10.9.60.61:9000/score"));
		
		
		FileOutputFormat.setOutputPath(
				job,new Path("hdfs://10.9.60.61:9000/score/result"));
		
		job.waitForCompletion(true);
		
	}
}
