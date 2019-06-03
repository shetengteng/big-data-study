package com.stt.demo.mr.Ch06_CustomizedInputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 读取小文件合并成一个文件
 * Created by Administrator on 2019/6/4.
 */
public class WholeFileInputFormat extends FileInputFormat<Text,BytesWritable> {

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// 不进行分片操作
		return false;
	}

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// 使用自定义的RecordReader
		WholeRecordReader recordReader = new WholeRecordReader();
		recordReader.initialize(split,context);
		return recordReader;
	}
}
