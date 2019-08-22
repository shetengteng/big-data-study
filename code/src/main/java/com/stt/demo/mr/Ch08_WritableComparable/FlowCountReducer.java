package com.stt.demo.mr.Ch08_WritableComparable;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<FlowBean,Text,Text,FlowBean> {
	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// 已经排好序了，输出即可
		for(Text v : values){
			context.write(v,key);
		}
	}
}
