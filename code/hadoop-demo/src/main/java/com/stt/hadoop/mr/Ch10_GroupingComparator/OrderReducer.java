package com.stt.hadoop.mr.Ch10_GroupingComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable> {

	@Override
	protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		// 获取前2名
		int topN = 2;
		for(NullWritable v : values){
			if(topN <= 0){
				break;
			}
			context.write(key,NullWritable.get());
			topN --;
		}
	}
}