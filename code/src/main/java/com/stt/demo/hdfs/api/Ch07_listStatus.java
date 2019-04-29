package com.stt.demo.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;

/**
 * Created by Administrator on 2019/4/29.
 */
public class Ch07_listStatus {

	@Test
	public void handle() throws Exception{

		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),new Configuration(),"ttshe");
		FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : fileStatuses) {
			if(fileStatus.isDirectory()){
				System.out.println("d:"+fileStatus.getPath().getName());
			}
			if(fileStatus.isFile()){
				System.out.println("f:"+fileStatus.getPath().getName());
			}
		}
	}

}
