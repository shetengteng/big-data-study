package com.stt.demo.mr.Ch16_MultiJob;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OneIndexReducer extends Reducer<Text,IntWritable,Text,NullWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		int sum = 0;
		for(IntWritable val : values){
			sum += val.get();
		}
		key.set(key.toString()+"-->"+sum);
		context.write(key,NullWritable.get());
	}
}
