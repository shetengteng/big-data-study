package com.stt.demo.mr.Ch08_WritableComparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static com.stt.demo.mr.Constant.INPUT_PATH_PREFIX;
import static com.stt.demo.mr.Constant.OUTPUT_PATH_PREFIX;

public class FlowCountDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		args = new String[]{INPUT_PATH_PREFIX+"ch08/input.txt", OUTPUT_PATH_PREFIX+"ch08/output"};

		// 配置信息以及job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(FlowCountDriver.class);

		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);

		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		// 设置自定义 分区操作类
		job.setPartitionerClass(NumberPartitioner.class);
		// 设置分区的个数
		job.setNumReduceTasks(5);

		boolean re = job.waitForCompletion(true);
		System.exit(re ? 0 : 1);
	}

}