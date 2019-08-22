package com.stt.demo.mr.Ch11_CustomizedOutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import static com.stt.demo.mr.Constant.OUTPUT_PATH_PREFIX;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

	private FSDataOutputStream output1 = null;
	private FSDataOutputStream output2 = null;

	public FilterRecordWriter(TaskAttemptContext context) {
		try{
			FileSystem fs = FileSystem.get(context.getConfiguration());
			output1 = fs.create(new Path(OUTPUT_PATH_PREFIX+"ch11/output/atguigu.log"));
			output2 = fs.create(new Path(OUTPUT_PATH_PREFIX+"ch11/output/other.log"));
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		FSDataOutputStream output = key.toString().contains("atguigu") ? output1 : output2;
		output.write(key.toString().getBytes());
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		IOUtils.closeStream(output1);
		IOUtils.closeStream(output2);
	}
}