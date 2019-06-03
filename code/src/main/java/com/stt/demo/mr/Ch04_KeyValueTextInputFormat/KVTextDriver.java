package com.stt.demo.mr.Ch04_KeyValueTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KVTextDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		args = new String[]{"d:/study/big-data/code/data/hadoop/mr/ch04/input.txt",
		"d:/study/big-data/code/data/hadoop/mr/ch04/output.txt"};

		Configuration config = new Configuration();
		// 设置分隔符
		config.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
		// 设置job
		Job job = Job.getInstance(config);
		job.setJarByClass(KVTextDriver.class);
		job.setMapperClass(KVTextMapper.class);
		job.setReducerClass(KVTextReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		// 设置输入格式
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);
	}

}
