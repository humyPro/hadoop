package com.humy.mapreduce.score;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SocreMapper extends Mapper<LongWritable, Text, Text, Score> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		//String
		String line=ivalue.toString();
		String name=line.split(" ")[1];
		int score=Integer.parseInt(line.split(" ")[2]);
		Score s=new Score();
		FileSplit split=(FileSplit)context.getInputSplit();
		String fileName=split.getPath().getName();
		System.err.println(name+":"+score);
		if("chinese.txt".equals(fileName)){
			s.setChinese(score);
		}else if("math.txt".equals(fileName)){
			s.setMath(score);
		}else if("english.txt".equals(fileName)){
			s.setEnglish(score);
		}
		s.setName(name);
		context.write(new Text(name), s);
	}

}
