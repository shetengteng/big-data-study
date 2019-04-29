package com.stt.demo.hdfs.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.net.URI;

/**
 * Created by Administrator on 2019/4/29.
 */
public class Ch06_listFiles {

	@Test
	public void handle() throws Exception{

		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),
				new Configuration(),"ttshe");
		// 获取文件详细情况
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/"), true);
		while(iter.hasNext()){
			LocatedFileStatus status = iter.next();
			System.out.println("文件名："+status.getPath().getName());
			System.out.println("路径："+status.getPath().getParent());
			System.out.println("长度："+status.getLen());
			System.out.println("权限："+status.getPermission());
			System.out.println("分组："+status.getGroup());
			// 获取存储的块信息
			BlockLocation[] blockLocations = status.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				// 获取块存储的主机节点
				String[] hosts = blockLocation.getHosts();
				for(String host:hosts){
					System.out.println("所在主机："+host);
				}
			}
			System.out.println("------------------------------------");
		}
		fs.close();
	}

}
