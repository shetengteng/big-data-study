package com.stt.hadoop.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;

/**
 * Created by Administrator on 2019/4/29.
 */
public class Ch03_copyToLocalFile {

	@Test
	public void handle() throws Exception {

		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),new Configuration(),"ttshe");
		// 执行下载操作
		// boolean delSrc 是否删除原文件
		// Path src 要下载的文件路径
		// Path dst 文件下载到的炉具
		// boolean useRawLocalFileSystem 使用本地文件系统，则没有文件校验CRC，否则会生成CRC文件
		fs.copyToLocalFile(
				false,
				new Path("/java/api/test/copyFromLocalFile.txt"),
				new Path("d:/d2.txt"),
				true);
		fs.close();
	}

	@Test
	public void handle2() throws Exception {

		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),new Configuration(),"ttshe");
		fs.copyToLocalFile(
				false,
				new Path("/java/api/test/copyFromLocalFile.txt"),
				new Path("d:/d3.txt"),
				false);
		fs.close();
	}


}
