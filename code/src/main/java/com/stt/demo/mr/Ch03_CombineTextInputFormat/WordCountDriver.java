package com.stt.demo.mr.Ch03_CombineTextInputFormat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 驱动类
 * Created by Administrator on 2019/5/5.
 */
public class WordCountDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		args = new String[]{"d:/study/big-data/code/data/hadoop/mr/ch03",
				"d:/study/big-data/code/data/hadoop/mr/ch03/output"};

		// 获取相应的配置服务
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		// 设置jar加载路径
		job.setJarByClass(WordCountDriver.class);
		// 设置map和reduce类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		// 设置map输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// 设置最终输出的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 默认使用TextFileInputFormat
		job.setInputFormatClass(CombineTextInputFormat.class);
		// 大小设置为4M
		CombineTextInputFormat.setMaxInputSplitSize(job,4194304);

		// 设置输入和输出路径
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		// 提交:查看源码，true表示监控job的运行情况，并打印
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}


}
