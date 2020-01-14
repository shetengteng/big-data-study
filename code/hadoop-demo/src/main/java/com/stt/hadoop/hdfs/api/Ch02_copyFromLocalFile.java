package com.stt.hadoop.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 从本地拷贝文件上传
 * Created by Administrator on 2019/4/29.
 */
public class Ch02_copyFromLocalFile {


	@Test
	public void handle1() throws URISyntaxException, IOException, InterruptedException {
		// 配置
		Configuration configuration = new Configuration();
		// 设置副本个数
		configuration.set("dfs.replication","2");
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration,"ttshe");
		// 上传文件
		fileSystem.copyFromLocalFile(new Path("d:/d.txt"),new Path("/java/api/test/copyFromLocalFile.txt"));
		// 关闭资源
		fileSystem.close();
	}

	/**
	 * 使用resource中的副本配置
	 */
	@Test
	public void handle2() throws IOException, URISyntaxException, InterruptedException {

		FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),new Configuration(),"ttshe");
		// 上传文件
		fileSystem.copyFromLocalFile(new Path("d:/d.txt"),new Path("/java/api/test/copyFromLocalFile2.txt"));
		// 关闭资源
		fileSystem.close();
	}

}
