package com.stt.demo.mr.Ch12_ReduceJoin;

import com.stt.demo.mr.Ch11_CustomizedOutputFormat.FilterDriver;
import com.stt.demo.mr.Ch11_CustomizedOutputFormat.FilterMapper;
import com.stt.demo.mr.Ch11_CustomizedOutputFormat.FilterOutputFormat;
import com.stt.demo.mr.Ch11_CustomizedOutputFormat.FilterReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static com.stt.demo.mr.Constant.INPUT_PATH_PREFIX;
import static com.stt.demo.mr.Constant.OUTPUT_PATH_PREFIX;

/**
 * Created by Administrator on 2019/8/22.
 */
public class TableDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		args = new String[]{INPUT_PATH_PREFIX+"ch12/input", OUTPUT_PATH_PREFIX+"ch12/output"};

		Job job = Job.getInstance(new Configuration());

		job.setJarByClass(TableDriver.class);

		job.setMapperClass(TableMapper.class);
		job.setReducerClass(TableReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TableBean.class);
		job.setOutputKeyClass(TableBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}