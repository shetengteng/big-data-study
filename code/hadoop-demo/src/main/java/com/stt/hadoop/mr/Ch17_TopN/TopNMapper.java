package com.stt.hadoop.mr.Ch17_TopN;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopNMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

	FlowBean v;
	Text k;
	// 使用treeMap对FlowBean进行排序过滤
	TreeMap<FlowBean,Text> treeMap = new TreeMap<>();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\s+");
		v = new FlowBean(Long.parseLong(fields[1]),Long.parseLong(fields[2]),Long.parseLong(fields[3]));
		k = new Text(fields[0]);
		treeMap.put(v,k);
		if(treeMap.size() > 10){
			treeMap.pollLastEntry();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for(Map.Entry<FlowBean,Text> entry : treeMap.entrySet()){
			context.write(entry.getValue(),entry.getKey());
		}
		treeMap.clear();
	}
}