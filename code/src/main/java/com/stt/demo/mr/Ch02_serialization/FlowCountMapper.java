package com.stt.demo.mr.Ch02_Serialization;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Administrator on 2019/5/14.
 */
public class FlowCountMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

	FlowBean v = new FlowBean();
	Text k = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 获取一行数据
		String line = value.toString();
		// 切割字段
		String[] fields = line.split("\t");
		// 封装对象
		String phoneNum = fields[1];
		// 取得上流量和下流量
		int len = fields.length;
		long upFlow = Long.parseLong(fields[len - 3]);
		long downFlow = Long.parseLong(fields[len - 2]);

		k.set(phoneNum);
		v.setDownFlow(downFlow);
		v.setUpFlow(upFlow);

		// 写出
		context.write(k,v);
	}
}
