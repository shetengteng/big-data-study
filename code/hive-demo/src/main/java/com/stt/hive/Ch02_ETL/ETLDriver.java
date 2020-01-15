package com.stt.hive.Ch02_ETL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ETLDriver implements Tool {

	private Configuration conf = null;

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		conf.set("inputpath",args[0]);
		conf.set("outputpath",args[1]);

		Job job = Job.getInstance(conf);

		job.setJarByClass(ETLDriver.class);

		job.setMapperClass(ETLMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		// 没有reduceTask
		job.setNumReduceTasks(0);

		initJobInputPath(job);
		initJobOutputPath(job);

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

	private void initJobOutputPath(Job job) throws IOException {

		Configuration conf = job.getConfiguration();

		String outputStr = conf.get("outputpath");
		Path out = new Path(outputStr);
		FileSystem fs = FileSystem.get(conf);
		// 如果存在则删除结果
		if(fs.exists(out)){
			fs.delete(out,true);
		}
		FileOutputFormat.setOutputPath(job,out);

	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ETLDriver(), args));
	}
}