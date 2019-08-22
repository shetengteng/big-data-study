package com.stt.demo.mr.Ch14_ETL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(parseLog(value)){
			context.write(value,NullWritable.get());
		}
	}

	private boolean parseLog(Text value) {
		return value.toString().split("\\s+").length > 11 ;
	}
}
