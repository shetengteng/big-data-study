package com.stt.demo.Ch02_mr01;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ReadDataMapper extends TableMapper<ImmutableBytesWritable,Put> {

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		// 获取rowKey
		byte[] rowKey = key.get();
		// 定义Put
		Put put = new Put(rowKey);
		// 将fruit中name和color进行提取，将每一行数据提出出来放入Put中
		for (Cell cell : value.rawCells()) {
			// 判断列族是需要的info中的name和color，其他的过滤
			if("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
				if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
					put.add(cell);
				}
				if("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
					put.add(cell);
				}
			}
			put.add(cell);
		}
		// 将从fruit读取的数据的每一行写入到context中作为map的输出
		if(!put.isEmpty()){
			context.write(key,put);
		}
	}
}
