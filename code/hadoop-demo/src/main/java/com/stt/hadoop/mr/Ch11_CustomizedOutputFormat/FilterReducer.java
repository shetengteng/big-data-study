package com.stt.hadoop.mr.Ch11_CustomizedOutputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		// 针对相同的key也进行输出
		for(NullWritable val : values){
			key.set(key.toString()+"\r\n");
			context.write(key,NullWritable.get());
		}
	}
}