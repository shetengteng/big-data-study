package com.stt.demo.hbase.Ch02_mr02;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReadDataMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] split = value.toString().split("\t");

		byte[] rowKeyByte=Bytes.toBytes(split[0]);

		byte[] family = Bytes.toBytes("info");
		byte[] name = Bytes.toBytes("name");
		byte[] color = Bytes.toBytes("color");
		byte[] nameVal = Bytes.toBytes(split[1]);
		byte[] colorVal = Bytes.toBytes(split[2]);

		Put put = new Put(rowKeyByte)
				.addColumn(family,name,nameVal)
				.addColumn(family,color,colorVal);

		ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowKeyByte);

		context.write(rowKey,put);
	}
}
