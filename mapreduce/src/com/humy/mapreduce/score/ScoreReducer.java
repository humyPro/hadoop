package com.humy.mapreduce.score;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreReducer extends Reducer<Text, Score, Text, Score> {

	public void reduce(Text _key, Iterable<Score> values, Context context) throws IOException, InterruptedException {
		// process values
		int chinese=0;
		int math=0;
		int english=0;
		for (Score val : values) {
			chinese=chinese+val.getChinese();
			math=math+val.getMath();
			english=english+val.getEnglish();
		}
		Score score = new Score();
		score.setName(_key.toString());
		score.setChinese(chinese);
		score.setMath(math);
		score.setEnglish(english);
		context.write(_key, score);
	}

}
