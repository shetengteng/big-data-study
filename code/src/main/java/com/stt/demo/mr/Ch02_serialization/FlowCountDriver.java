package com.stt.demo.mr.Ch02_serialization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static com.stt.demo.mr.Constant.INPUT_PATH_PREFIX;
import static com.stt.demo.mr.Constant.OUTPUT_PATH_PREFIX;

/**
 * Created by Administrator on 2019/5/19.
 */
public class FlowCountDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 设置输入输出参数

		args = new String[]{INPUT_PATH_PREFIX+"ch02/input.txt", OUTPUT_PATH_PREFIX+"ch02/output"};

		// 配置信息以及job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(FlowCountDriver.class);

		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		boolean re = job.waitForCompletion(true);
		System.exit(re ? 0 : 1);
	}

}
