package com.stt.demo.mr.Ch08_WritableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 注意这里的key是FlowBean ,value是Text类型的手机号
 */
public class FlowCountMapper extends Mapper<LongWritable,Text,FlowBean,Text> {

	FlowBean k = new FlowBean();
	Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 获取一行数据
		String line = value.toString();
		// 切割字段
		String[] fields = line.split("\t");
		// 封装对象
		String phoneNum = fields[0];
		// 取得上流量和下流量
		long upFlow = Long.parseLong(fields[1]);
		long downFlow = Long.parseLong(fields[2]);
		long sumFlow = Long.parseLong(fields[3]);

		v.set(phoneNum);
		k.setDownFlow(downFlow);
		k.setUpFlow(upFlow);
		k.setSumFlow(sumFlow);

		context.write(k,v);
	}
}
