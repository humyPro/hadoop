package com.humy.mapreduce.flow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlowDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(FlowDriver.class);
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Flow.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Flow.class);
		
		FileInputFormat.setInputPaths(
				job,new Path("hdfs://10.9.60.61:9000/flow"));
		
		
		FileOutputFormat.setOutputPath(
				job,new Path("hdfs://10.9.60.61:9000/flow/result"));
		
		job.waitForCompletion(true);
	}
}
