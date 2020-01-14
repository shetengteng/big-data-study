package com.stt.hadoop.mr.Ch04_KeyValueTextInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KVTextMapper extends Mapper<Text,Text,Text,LongWritable> {

	LongWritable v = new LongWritable(1);

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		context.write(key,v);
	}
}