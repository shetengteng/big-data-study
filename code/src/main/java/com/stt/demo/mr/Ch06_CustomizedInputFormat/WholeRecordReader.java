package com.stt.demo.mr.Ch06_CustomizedInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by Administrator on 2019/6/4.
 */
public class WholeRecordReader extends RecordReader<Text,BytesWritable> {

	private Configuration configuration;
	private FileSplit fileSplit;

	private boolean isProgress = true;

	private BytesWritable v = new BytesWritable();
	private Text k = new Text();

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		configuration = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// 标记位，表示是否读取完成，注意查看Mapper中的run方法的while
		if(isProgress){

			// 获取文件系统
			FileSystem fs = FileSystem.get(configuration);
			// 获取切片的输入流
			FSDataInputStream fsDataInputStream = fs.open(fileSplit.getPath());
			// 定义缓存区
			byte[] buf = new byte[(int) fileSplit.getLength()];
			// 拷贝数据切片的所有数据到buf
			IOUtils.readFully(fsDataInputStream,buf,0,buf.length);
			// 封装value
			v.set(buf,0,buf.length);
			// 封装key，获取文件的路径和名称
			k.set(fileSplit.getPath().toString());

			IOUtils.closeStream(fsDataInputStream);

			isProgress = false;
			return true;

		}
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return k;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return v;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
		// 关闭一些资源
	}
}
