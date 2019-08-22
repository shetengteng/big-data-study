package com.stt.demo.mr.Ch07_partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static com.stt.demo.mr.Constant.*;

/**
 * Created by Administrator on 2019/5/19.
 */
public class FlowCountDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		args = new String[]{INPUT_PATH_PREFIX+"ch07/input.txt", OUTPUT_PATH_PREFIX+"ch07/output"};

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
		// 设置自定义 分区操作类
		job.setPartitionerClass(NumberPartitioner.class);
		// 设置分区的个数
		job.setNumReduceTasks(5);

		boolean re = job.waitForCompletion(true);
		System.exit(re ? 0 : 1);
	}

}
