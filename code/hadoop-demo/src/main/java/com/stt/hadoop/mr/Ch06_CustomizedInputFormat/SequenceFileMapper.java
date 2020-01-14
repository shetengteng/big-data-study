package com.stt.hadoop.mr.Ch06_CustomizedInputFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SequenceFileMapper extends Mapper<Text,BytesWritable,Text,BytesWritable> {

	@Override
	protected void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		context.write(key,value);
	}
}
