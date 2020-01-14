package com.stt.hadoop.mr.Ch12_ReduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


public class TableMapper extends Mapper<LongWritable,Text,Text,TableBean> {

	private String fileName;

	TableBean v = new TableBean();
	Text k = new Text();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// 获取切片的名称
		fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\s+");

		if(fileName.startsWith("order")){
			// 1001	01	1
			v.setOrderId(fields[0]);
			v.setPId(fields[1]);
			v.setAmount(Integer.parseInt(fields[2]));
			v.setFlag("order");
			v.setPName("");
		}else if(fileName.startsWith("pd")){
			// 01	小米
			v.setPId(fields[0]);
			v.setPName(fields[1]);
			v.setFlag("pd");
			v.setAmount(0);
			v.setOrderId("");
		}
		k.set(v.getPId());
		context.write(k,v);
	}
}