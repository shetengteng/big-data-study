package com.stt.demo.Ch02_mr01;


import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class WriteDataReducer extends TableReducer<ImmutableBytesWritable,Put,NullWritable> {

	@Override
	protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
		// 读出来的每一行数据写入到fruit_mr表中
		for (Put value : values) {
			context.write(NullWritable.get(),value);
		}
	}
}
