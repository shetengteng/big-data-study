package com.stt.hadoop.mr.Ch08_WritableComparable;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 用于统计流量的bean
 * Created by Administrator on 2019/5/14.
 */
@Data
public class WritableComparableFlowBean implements WritableComparable<WritableComparableFlowBean>{

	private long upFlow;
	private long downFlow;
	private long sumFlow;

	// 反序列化时，需要反射调用空参构造函数
	public WritableComparableFlowBean(){
		super();
	}

	public WritableComparableFlowBean(long upFlow, long downFlow){
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}

	// 写序列化方法
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}

	// 反序列化方法
	@Override
	public void readFields(DataInput in) throws IOException {
		// 反序列化方法必须要和序列化方法的执行顺序保持一致
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
	}

	public String toString(){
		return upFlow+"\t"+downFlow+"\t"+sumFlow;
	}

	@Override
	public int compareTo(WritableComparableFlowBean o) {
		// 按照流量总大小倒叙排列
		if(Objects.equals(sumFlow,o.getSumFlow())){
			return 0;
		}
		return sumFlow > o.getSumFlow() ? -1 : 1;
	}
}