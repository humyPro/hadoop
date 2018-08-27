package com.humy.mapreduce.flow;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<Text, Flow, Text, Flow>{

	@Override
	protected void reduce(Text key, Iterable<Flow> values, Reducer<Text, Flow, Text, Flow>.Context context)
			throws IOException, InterruptedException {
		Flow result=new Flow();
		for(Flow val:values){
			result.setName(val.getName());
			result.setPhone(val.getPhone());
			result.setAddr(val.getAddr());
			result.setFlow(result.getFlow()+val.getFlow());
		}
		context.write(key, result);
		
		
	}
	

}
