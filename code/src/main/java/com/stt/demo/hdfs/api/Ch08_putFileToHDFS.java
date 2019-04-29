package com.stt.demo.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;

/**
 * Created by Administrator on 2019/4/29.
 */
public class Ch08_putFileToHDFS {

	@Test
	public void handle() throws Exception{

		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(
				new URI("hdfs://hadoop102:9000"),conf,"ttshe");
		// 创建输入流
		FileInputStream fis = new FileInputStream(new File("d:/d.txt"));
		// 获取输出流
		FSDataOutputStream fos = fs.create(new Path("/java/api/test/putToHDFS.txt"));
		// 拷贝流
		IOUtils.copyBytes(fis,fos,conf);
		// 关闭流
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();

	}

}
