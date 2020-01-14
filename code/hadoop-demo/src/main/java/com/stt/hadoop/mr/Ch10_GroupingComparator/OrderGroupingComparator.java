package com.stt.hadoop.mr.Ch10_GroupingComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.util.Objects;

public class OrderGroupingComparator extends WritableComparator {

	// 这里必须要传递true，用于实例化，否则报异常
	protected OrderGroupingComparator(){
		super(OrderBean.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// 在相同的id处进行分组处理
		// 由于在MapTask阶段进行了二次排序
		// 到此处compare的是排序后的key对象
		OrderBean aOrder = (OrderBean) a;
		OrderBean bOrder = (OrderBean) b;

		if(Objects.equals(aOrder.getOrderId(),bOrder.getOrderId())){
			return 0;
		}
		return aOrder.getOrderId() > bOrder.getOrderId() ? 1 : -1;
	}
}
