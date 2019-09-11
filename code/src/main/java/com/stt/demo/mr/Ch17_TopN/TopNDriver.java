package com.stt.demo.mr.Ch17_TopN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static com.stt.demo.mr.Constant.*;

public class TopNDriver {

	public static void main(String[] args) throws Exception {

		args = new String[]{INPUT+"ch17/input.txt", OUTPUT+"ch17/output"};

		Job job = Job.getInstance(new Configuration());

		job.setJarByClass(TopNDriver.class);

		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}