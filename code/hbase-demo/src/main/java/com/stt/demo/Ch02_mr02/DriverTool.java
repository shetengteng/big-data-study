package com.stt.demo.Ch02_mr02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class DriverTool extends Configuration implements Tool{

	private Configuration conf;

	@Override
	public int run(String[] args) throws Exception {
		getConf().set("inputpath",args[0]);

		Job job = Job.getInstance(getConf());
		// 设置当前jar包通过那个类读取
		job.setJarByClass(DriverTool.class);
		// 至少1个
		job.setNumReduceTasks(1);

		// 设置Mapper
		job.setMapperClass(ReadDataMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		// 设置reducer
		TableMapReduceUtil.initTableReducerJob(
				"fruit_mr",// 目标的表名
				WriteDataReducer.class,
				job
		);

		initJobInputPath(job);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	private void initJobInputPath(Job job) throws IOException {

		Configuration conf = job.getConfiguration();

		// 对输入路径进行判断是否存在，不存在抛出异常
		FileSystem fs = FileSystem.get(conf);
		String inputPathStr = conf.get("inputpath");
		Path in = new Path(inputPathStr);
		if(!fs.exists(in)){
			throw new RuntimeException(inputPathStr + " is not exists");
		}
		FileInputFormat.addInputPath(job,in);
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new DriverTool(), args));
	}
}
