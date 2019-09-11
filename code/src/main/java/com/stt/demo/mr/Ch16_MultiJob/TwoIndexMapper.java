package com.stt.demo.mr.Ch16_MultiJob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TwoIndexMapper extends Mapper<LongWritable,Text,Text,Text> {

	private Text k = new Text();
	private Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//atguigu**a.txt-->3
		String[] fields = value.toString().split("\\*+");
		k.set(fields[0]);
		v.set(fields[1]);
		context.write(k,v);
	}
}
