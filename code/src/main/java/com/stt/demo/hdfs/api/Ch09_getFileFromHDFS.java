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
 * 使用IO的api获取hdfs资源
 * Created by Administrator on 2019/4/29.
 */
public class Ch09_getFileFromHDFS {

	@Test
	public void handle() throws Exception{

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"ttshe");
		// 获取输入流
		FSDataInputStream fis = fs.open(new Path("/java/api/test/putToHDFS.txt"));
		// 获取输出流
		FileOutputStream fos = new FileOutputStream(new File("d:/d3.txt"));
		IOUtils.copyBytes(fis,fos,conf);
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}
}
