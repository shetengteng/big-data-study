package com.stt.demo.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;

/**
 * Created by Administrator on 2019/4/29.
 */
public class Ch05_rename {

	@Test
	public void handle() throws Exception{
		FileSystem fs = FileSystem.get(
				new URI("hdfs://hadoop102:9000"),
				new Configuration(),"ttshe");
		fs.rename(new Path("/java/api/test/mkdir"),
				new Path("/java/api/test/rename"));
		fs.close();
	}

}
