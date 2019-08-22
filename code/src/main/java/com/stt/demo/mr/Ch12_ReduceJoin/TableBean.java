package com.stt.demo.mr.Ch12_ReduceJoin;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
public class TableBean implements Writable {

	private String orderId; // 订单id
	private String pId;      // 产品id
	private int amount;       // 产品数量
	private String pName;     // 产品名称
	private String flag;      // 表的标记

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeUTF(orderId);
		out.writeUTF(pId);
		out.writeInt(amount);
		out.writeUTF(pName);
		out.writeUTF(flag);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		this.orderId = in.readUTF();
		this.pId = in.readUTF();
		this.amount = in.readInt();
		this.pName = in.readUTF();
		this.flag = in.readUTF();
	}

	public String toString(){
		return orderId + "\t" + pName + "\t" + amount;
	}
}
