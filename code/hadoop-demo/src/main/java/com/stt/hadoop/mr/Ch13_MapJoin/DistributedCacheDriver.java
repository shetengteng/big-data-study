package com.stt.hadoop.mr.Ch13_MapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

import static com.stt.hadoop.Constant.PATH;

public class DistributedCacheDriver  {

	public static void main(String[] args) throws Exception {

		args = new String[]{PATH+"ch13/order.txt", PATH+"ch13/output"};

		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(DistributedCacheDriver .class);
		job.setMapperClass(DistributedCacheMapper.class);

		// 设置为0，没有Reducer阶段处理
		job.setNumReduceTasks(0);
		// 增加缓存文件路径，map阶段从setup读取
		job.addCacheFile(new URI("file:///"+PATH+"ch13/pd.txt"));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}