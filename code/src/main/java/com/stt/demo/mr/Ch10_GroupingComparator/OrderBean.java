package com.stt.demo.mr.Ch10_GroupingComparator;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

@Data
public class OrderBean implements WritableComparable<OrderBean>{

	private int orderId;
	private double price;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(orderId);
		out.writeDouble(price);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		orderId = in.readInt();
		price = in.readDouble();
	}

	public String toString(){
		return orderId + "\t" + price;
	}

	@Override
	public int compareTo(OrderBean o) {

		if(Objects.equals(orderId,o.getOrderId())){
			if(Objects.equals(price,o.getPrice())){
				return 0;
			}
			// 按照价格降序
			return price > o.getPrice() ? -1 : 1;
		}

		// 按照orderId默认升序
		return orderId > o.getOrderId() ? 1 : -1;
	}
}
