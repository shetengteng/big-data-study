package com.stt.hadoop.mr.Ch13_MapJoin;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DistributedCacheMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

	Map<String,String> pdMap = new HashMap<>();
	Text k = new Text();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// 从缓存中读取pd表信息
		List<String> lines = Files.readAllLines(Paths.get(context.getCacheFiles()[0]), Charsets.UTF_8);
		for(String line : lines){
			// 01	小米
			String[] fields = line.split("\\s+");
			pdMap.put(fields[0],fields[1]);
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\\s+");
		k.set(fields[0] + "\t" + pdMap.get(fields[1]) + "\t" + fields[2]);
		context.write(k, NullWritable.get());
	}
}