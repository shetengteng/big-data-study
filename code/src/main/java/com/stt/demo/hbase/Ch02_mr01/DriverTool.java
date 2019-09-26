package com.stt.demo.hbase.Ch02_mr01;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverTool extends Configuration implements Tool{

	private Configuration conf;

	@Override
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf());
		// 设置当前jar包通过那个类读取
		job.setJarByClass(DriverTool.class);
		// 至少1个
		job.setNumReduceTasks(1);

		// 配置Scan
		Scan scan = new Scan().setCacheBlocks(false).setCaching(500);

		// 设置Mapper，注意导入的是mapreduce包下的，不是mapred包下的，后者是老版本
		TableMapReduceUtil.initTableMapperJob(
				"fruit",// 数据源表名
				scan,
				ReadDataMapper.class,
				ImmutableBytesWritable.class, // Mapper的输出Key
				Put.class,// Mapper的输出value
				job
		);

		// 设置reducer
		TableMapReduceUtil.initTableReducerJob(
				"fruit_mr",// 目标的表名
				WriteDataReducer.class,
				job
		);

		return job.waitForCompletion(true) ? 0 : 1;
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

		// 可以不设置conf，由于有hbase-site.xml
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
		System.exit(ToolRunner.run(conf,new DriverTool(), args));
	}
}
