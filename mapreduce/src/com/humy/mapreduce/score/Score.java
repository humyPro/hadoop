package com.humy.mapreduce.score;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Score implements Writable{
	
	private String name;
	private int chinese;
	private int math;
	private int english;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		name=in.readUTF();
		chinese=in.readInt();
		math=in.readInt();
		english=in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(chinese);
		out.writeInt(math);
		out.writeInt(english);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getChinese() {
		return chinese;
	}

	public void setChinese(int chinese) {
		this.chinese = chinese;
	}

	public int getMath() {
		return math;
	}

	public void setMath(int math) {
		this.math = math;
	}

	public int getEnglish() {
		return english;
	}

	public void setEnglish(int english) {
		this.english = english;
	}

	@Override
	public String toString() {
		return "Score [name=" + name + ", chinese=" + chinese + ", math=" + math + ", english=" + english + "]";
	}
	

}
