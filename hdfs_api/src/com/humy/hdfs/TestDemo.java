package com.humy.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class TestDemo {
	@Test
	public void connect() throws Exception{
		Configuration conf=new Configuration();
		//ͨ连接hadoop
		conf.set("dfs.replication", "1");
		FileSystem fs=FileSystem.get(new URI("hdfs://10.9.60.61:9000"),conf);
		
		fs.close();
		
	}
	//获取文件
	@Test
	public void get() throws IOException, URISyntaxException{
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(new URI("hdfs://10.9.60.61:9000"),conf);
		FSDataInputStream in = fs.open(new Path("/park01/1.txt"));
		FileOutputStream out = new FileOutputStream("data.txt");
		//hadoop下的一个工具类，可以直接从输入流写到输出流
		IOUtils.copyBytes(in, out, conf);
		in.close();
		out.close();
		fs.close();
	}
	
	//上传文件
	@Test
	public void put() throws IOException, URISyntaxException{
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(new URI("hdfs://10.9.60.61:9000"),conf);
		FileInputStream in = new FileInputStream(new File("data.txt"));
		FSDataOutputStream out = fs.create(new Path("/park02/data.txt"));
		
		IOUtils.copyBytes(in, out, conf);
		in.close();
		out.close();
		fs.close();
		
		
	}
	
	@Test
	public void getBlockLocation() throws IOException, URISyntaxException{
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(new URI("hdfs://10.9.60.61:9000"),conf);
		
		//fs.getFileBlockLocations(new Path(""), start, len);
	}
	
}
