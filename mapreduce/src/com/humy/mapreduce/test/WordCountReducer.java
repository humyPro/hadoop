package com.humy.mapreduce.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * 知识点：
 * 1、reduce组件用于接收mapper组件的输出
 * 2、reduce第一个泛型类型是reduce的输入key，需要和mapper中的输出key保持一致
 * 3、第二个泛型类型是reduce的输出value，需要和mapper的输出value类型一致
 * 4、第三个泛型是reduce的输出key类型，根据具体业务决定
 * 5、第四个泛型类型是reduce的输出value类型，根据具体业务决定
 * 6、reduce收到mapper的输出，会按照相同的key做聚合，形成key iterable形式后通过reduce方法传递程序员
 * @author humy
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text _key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// process values
		int count=0;
		for (IntWritable val : values) {
			count=count+val.get();
		}
		
		context.write(_key, new IntWritable(count));
		
	}
 
}
