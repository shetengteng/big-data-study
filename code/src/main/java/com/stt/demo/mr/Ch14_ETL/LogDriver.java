package com.stt.demo.mr.Ch14_ETL;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static com.stt.demo.mr.Constant.*;

public class LogDriver {

	public static void main(String[] args) throws Exception {

		args = new String[]{INPUT+"ch14/log.txt", OUTPUT+"ch14/output"};

		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(LogDriver.class);
		job.setMapperClass(LogMapper.class);

		// 设置为0，没有Reducer阶段处理
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
