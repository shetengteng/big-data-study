package com.stt.demo.mr.Ch06_CustomizedInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import static com.stt.demo.mr.Constant.INPUT_PATH_PREFIX;
import static com.stt.demo.mr.Constant.OUTPUT_PATH_PREFIX;

/**
 * Created by Administrator on 2019/6/4.
 */
public class SequenceFileDriver {

	public static void main(String[] args) throws Exception {
		args = new String[]{INPUT_PATH_PREFIX+"ch06", OUTPUT_PATH_PREFIX+"ch06/output"};

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(SequenceFileDriver.class);
		job.setMapperClass(SequenceFileMapper.class);
		job.setReducerClass(SequenceFileReducer.class);

		// 定义输入的InputFormat
		job.setInputFormatClass(WholeFileInputFormat.class);
		// 定义输出的OutputFormat
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		boolean re = job.waitForCompletion(true);
		System.exit(re ? 0:1);
	}
}
