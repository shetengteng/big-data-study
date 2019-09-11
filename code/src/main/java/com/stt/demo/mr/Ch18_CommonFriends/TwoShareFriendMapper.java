package com.stt.demo.mr.Ch18_CommonFriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class TwoShareFriendMapper extends Mapper<LongWritable,Text,Text,Text>{

	Text k = new Text();
	Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//		A	I,K,C,B,G,F,H,O,D,
//		转换为 key I-K,C-K value A
		String[] fields = value.toString().split("\\s+");
		String friend = fields[0];
		v.set(friend);

		String[] person = fields[1].split(",");
//	    对 value中的人进行排序 因为 I-K 和 K-I 都表示 I和K之间的共同好友的key
		Arrays.sort(person);
		int len = person.length;
		for(int i=0;i<len-1;i++) {
			for(int j=i+1;j<len;j++){
				k.set(person[i]+"-"+person[j]);
				context.write(k,v);
			}
		}
	}
}