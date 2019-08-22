package com.stt.demo.mr.Ch08_WritableComparable;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner 的 key 和 value 是 MapTask阶段 输出的结果类型
 */
public class NumberPartitioner extends Partitioner<FlowBean,Text> {
	@Override
	public int getPartition(FlowBean key, Text value, int numPartitions) {
		String phone = value.toString();
		if(StringUtils.isEmpty(phone)){
			// 默认输出到0分区
			return 0;
		}
		if(phone.startsWith("136")){
			return 1;
		}
		if(phone.startsWith("137")){
			return 2;
		}
		if(phone.startsWith("138")){
			return 3;
		}
		if(phone.startsWith("139")){
			return 4;
		}
		return 0;
	}
}
