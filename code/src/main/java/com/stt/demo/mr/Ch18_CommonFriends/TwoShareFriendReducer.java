package com.stt.demo.mr.Ch18_CommonFriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoShareFriendReducer extends Reducer<Text,Text,Text,Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		values.forEach(text -> sb.append(text.toString()).append(" "));
		context.write(key,new Text(sb.toString()));
	}
}