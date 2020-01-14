package com.stt.hadoop.mr.Ch06_CustomizedInputFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SequenceFileReducer extends Reducer<Text,BytesWritable,Text,BytesWritable> {

	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
		// values的值只有一个
		context.write(key,values.iterator().next());
	}
}
