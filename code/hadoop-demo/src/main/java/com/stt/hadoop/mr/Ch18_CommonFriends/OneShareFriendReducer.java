package com.stt.hadoop.mr.Ch18_CommonFriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OneShareFriendReducer extends Reducer<Text,Text,Text,Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		// key 是 好友对象
		// values 是 被好友对象
		// 此处的输出要求是 key 好友对象，value，被好友对象的集合，用,号隔开
		StringBuilder sb = new StringBuilder();
		values.forEach(val -> sb.append(val.toString()).append(","));
		context.write(key,new Text(sb.toString()));
	}
}
