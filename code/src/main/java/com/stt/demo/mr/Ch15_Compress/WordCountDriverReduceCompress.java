package com.stt.demo.mr.Ch15_Compress;

import com.stt.demo.mr.Ch01_WordCount.WordCountMapper;
import com.stt.demo.mr.Ch01_WordCount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static com.stt.demo.mr.Constant.INPUT;
import static com.stt.demo.mr.Constant.OUTPUT;


public class WordCountDriverReduceCompress {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		args = new String[]{INPUT+"ch15/wordcount.txt", OUTPUT+"ch15/output"};
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCountDriverReduceCompress.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		// 设置reduce端输出压缩开启
		FileOutputFormat.setCompressOutput(job, true);
		// 设置压缩的方式
		FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}