package com.humy.mapreduce.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WordCountDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//hadoop环境变量对象
		Configuration conf=new Configuration();
		Job job = Job.getInstance(conf);
		//设置方法入口主类
		job.setJarByClass(WordCountDriver.class);
		//设置Mapper组件类，底层通过反射机制来找到
		job.setMapperClass(WordCountMapper_1.class);
		//设置输出key类型/输出value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//设置reduce的类和输出类型
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//可以设置到文件级别，只处理这一个文件，也可以设置到目录级别，会处理目录下的所有文件
		FileInputFormat.setInputPaths(job, new Path("hdfs://10.9.60.61:9000/word"));
		//输出路径，此路径实现不能存在，否则会报错
		FileOutputFormat.setOutputPath(job,new Path("hdfs://10.9.60.61:9000/word/result") );
		
		//提交job到MapReduce集群
		job.waitForCompletion(true);
		
		
		
		
	}
}
