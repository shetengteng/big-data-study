package com.stt.demo.mr.Ch05_NLineInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NLineMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
	Text k = new Text();
	LongWritable v = new LongWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");
		for (String word : words) {
			k.set(word);
			context.write(k,v);
		}
	}
}
