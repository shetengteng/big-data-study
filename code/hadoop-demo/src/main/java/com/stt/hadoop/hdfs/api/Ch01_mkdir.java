package com.stt.hadoop.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 使用api的方式在hdfs上创建目录
 * Created by Administrator on 2019/4/25.
 */
public class Ch01_mkdir {


	/**
	 * 运行方式1，需要在VM options添加配置-DHADOOP_USER_NAME=ttshe
	 * @throws IOException
	 */
	@Test
	public void handle1() throws IOException {
		// 使用api的方式在hdfs上创建目录
		// 获取文件系统
		Configuration configuration = new Configuration();
		// 配置在集群上运行，注意这里配置的信息是core-site.xml上的NameNode的信息
		configuration.set("fs.defaultFS","hdfs://hadoop102:9000");
		// 设置文件系统用户
		FileSystem fs = FileSystem.get(configuration);
		// 创建目录
		fs.mkdirs(new Path("/java/api/test/mkdir01"));
		// 关闭资源
		fs.close();
	}

	/**
	 * 运行方式2
	 * @throws URISyntaxException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void handle2() throws URISyntaxException, IOException, InterruptedException {
		// 使用api的方式在hdfs上创建目录
		// 获取文件系统
		Configuration configuration = new Configuration();
		// 设置文件系统用户
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "ttshe");
		// 创建目录
		fs.mkdirs(new Path("/java/api/test/mkdir02"));
		// 关闭资源
		fs.close();
	}

}
