package com.stt.hadoop.mr.Ch09_Combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCombiner extends Reducer<Text,IntWritable,Text,IntWritable>  {

	IntWritable val = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable v : values){
			sum += v.get();
		}
		val.set(sum);
		context.write(key,val);
	}
}
