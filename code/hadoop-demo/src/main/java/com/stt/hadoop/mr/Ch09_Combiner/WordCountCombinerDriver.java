package com.stt.hadoop.mr.Ch09_Combiner;


import com.stt.hadoop.mr.Ch01_WordCount.WordCountMapper;
import com.stt.hadoop.mr.Ch01_WordCount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static com.stt.hadoop.Constant.PATH;

public class WordCountCombinerDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		args = new String[]{PATH+"ch09/input.txt", PATH+"ch09/output"};
		// 获取相应的配置服务
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		// 设置jar加载路径
		job.setJarByClass(WordCountCombinerDriver.class);
		// 设置map和reduce类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		// 登记combiner
		job.setCombinerClass(WordCombiner.class);

		// 设置map输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// 设置最终输出的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(1);

		// 设置输入和输出路径
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		// 提交:查看源码，true表示监控job的运行情况，并打印
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}

}