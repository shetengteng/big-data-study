package com.stt.demo.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * Created by Administrator on 2019/4/29.
 */
public class Ch10_readFileSeek {

	// 下载第一个块
	@Test
	public void handle1() throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(
				new URI("hdfs://hadoop102:9000"),conf,"ttshe");
		// 获取输入流
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
		// 创建输出流
		FileOutputStream fos = new FileOutputStream(new File("d:/h.part1"));
		// 流的拷贝
		byte[] buffer = new byte[1024];
		// 读取128Mb = 128*1024*1024
		for(int i=0;i<1024*128;i++){
			fis.read(buffer);
			fos.write(buffer);
		}
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}

	// 下载第二个块
	@Test
	public void handle2() throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(
				new URI("hdfs://hadoop102:9000"),conf,"ttshe");
		// 获取输入流
		FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
		// 创建输出流
		FileOutputStream fos = new FileOutputStream(new File("d:/h.part2"));

		//重点： 定位输入数据的位置
		fis.seek(1024*1024*128);

		IOUtils.copyBytes(fis,fos,conf);
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}
}
