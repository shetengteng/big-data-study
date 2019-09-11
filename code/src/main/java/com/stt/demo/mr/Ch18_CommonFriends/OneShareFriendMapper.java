package com.stt.demo.mr.Ch18_CommonFriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OneShareFriendMapper extends Mapper<LongWritable,Text,Text,Text> {

	Text k = new Text();
	Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//A:B,C,D,F,E,O
		// A 的单向好友是B，那么B的所有被单向好友有哪些
		// 理解成B的被哪些人关注
		String[] fields = value.toString().split(":");
		String person = fields[0];
		v.set(person);

		String[] friends = fields[1].split(",");
		for (String friend : friends) {
			k.set(friend);
			context.write(k,v);
		}
	}
}
