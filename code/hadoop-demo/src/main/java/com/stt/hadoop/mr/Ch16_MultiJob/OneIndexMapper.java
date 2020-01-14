package com.stt.hadoop.mr.Ch16_MultiJob;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OneIndexMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

	private String fileName;
	private IntWritable v = new IntWritable(1);
	private Text k = new Text();


	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//		atguigu pingping
//		转换为 atguigu**fileName.txt-->1
		String[] fields = value.toString().split("\\s+");
		for(String field : fields){
			k.set(field+"**"+fileName);
			context.write(k,v);
		}
	}
}
