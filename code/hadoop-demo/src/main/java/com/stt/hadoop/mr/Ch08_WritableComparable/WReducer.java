package com.stt.hadoop.mr.Ch08_WritableComparable;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WReducer extends Reducer<WritableComparableFlowBean,Text,Text,WritableComparableFlowBean> {
	@Override
	protected void reduce(WritableComparableFlowBean key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// 已经排好序了，输出即可
		// 注意迭代器的源码，这里被hadoop重写了
		for(Text v : values){
			context.write(v,key);
		}
	}
}
