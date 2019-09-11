package com.stt.demo.mr.Ch16_MultiJob;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoIndexReducer extends Reducer<Text,Text,Text,NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder(key.toString()).append("\t");
		for(Text val : values){
			sb.append(val.toString()).append("\t");
		}
		key.set(sb.toString());
		context.write(key,NullWritable.get());
	}
}
