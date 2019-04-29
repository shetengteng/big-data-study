package com.stt.demo.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;

/**
 * Created by Administrator on 2019/4/29.
 */
public class Ch04_delete {

	@Test
	public void handle() throws Exception {

		FileSystem fs = FileSystem.get(
				new URI("hdfs://hadoop102:9000"),new Configuration(),"ttshe");
		fs.delete(new Path("/dir01/"),true);
		fs.close();

	}

}
