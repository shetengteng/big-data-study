package com.stt.hadoop.mr.Ch08_WritableComparable;

import com.stt.hadoop.mr.Ch02_Serialization.FlowCountMapper;
import com.stt.hadoop.mr.Ch02_Serialization.FlowCountReducer;
import com.stt.hadoop.mr.Ch07_Partition.NumberPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import static com.stt.hadoop.Constant.PATH;

public class WDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		args = new String[]{PATH+"ch08/input.txt", PATH+"ch08/output"};

		// 配置信息以及job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(WDriver.class);

		job.setMapperClass(WMapper.class);
		job.setReducerClass(WReducer.class);

		job.setMapOutputKeyClass(WritableComparableFlowBean.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(WritableComparableFlowBean.class);

		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		// 设置自定义 分区操作类
		job.setPartitionerClass(WNumberPartitioner.class);
		// 设置分区的个数
		job.setNumReduceTasks(5);

		boolean re = job.waitForCompletion(true);
		System.exit(re ? 0 : 1);
	}

}