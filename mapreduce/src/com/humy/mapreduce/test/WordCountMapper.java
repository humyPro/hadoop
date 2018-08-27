package com.humy.mapreduce.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 知识点
 * 1、Mapper组件的开发方式，是写一个类，继承Mapper
 * 2、Mapper组件的作用是定义每一个MapTask具体要怎么处理数据
 * 比如一个文件，257M，会生成3个MapTask。即三个MapTask处理逻辑是一样的
 * 至少每个MapTask处理的数据不一样
 * 3、第一个泛型类型：LongWritable，对应Mapper的输入key，输入key是每行的行首偏移量
 * 4、第二个泛型类型：Text，对应mapper的输入value，即每行的内容
 * 5、第三个泛型类型：对应Mapper的输出key，这需要根据业务来定义
 * 6、第三个泛型类型：对应Mapper的输出value，这需要根据业务来定义
 * 7、注意，初学时，第一个和第二个泛型写死，第三个和第四个不固定
 * 8、Writable机制是Hadoop自身的序列化机制，常用的类型有：
 * 		LongWritable、Text(String)、Intwritable、NullWritable
 * 9、定义MapTast的业务逻辑是重写map()方法来实现的，读取一行数据就会调用一次这个方法，同时会把输入key和输入value传给程序员。
 * 在实际开发中，输入key很少用，最重要的是拿到输入value
 * 10、输出方法：通过context.write(输出key，输出value)
 * @author humy
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable>{

	@Override//前两个参数对应输入key和输入value，后两个是输出key和输出value
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//map方法的调用次数取决于文件块的行数
		//key：输入key--每行的行首偏移量
		//value：输入value--每行的内容
		//super.map(key, value, context);
		String line=value.toString();
		String[] words=line.split(" ");
//		Map<String,Integer> countMap=new HashMap<String,Integer>();
//		for(String word:words){
//			if(countMap.containsKey(word)){
//				countMap.put(word, countMap.get(word)+1);
//			}else{
//				countMap.put(word, 1);
//			}
//			//context.write(arg0, arg1);
//		}
		for(String word:words){
			context.write(new Text(word), new IntWritable(1));
		}
				
	}
	
}
