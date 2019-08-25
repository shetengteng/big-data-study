package com.stt.demo.mr.Ch07_Partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Administrator on 2019/5/14.
 */
public class FlowCountReducer extends Reducer<Text, FlowBean,Text, FlowBean> {
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

		long sum_upFlow = 0;
		long sum_downFlow = 0;

		for(FlowBean flowBean:values){
			sum_downFlow += flowBean.getDownFlow();
			sum_upFlow += flowBean.getUpFlow();
		}

		FlowBean result = new FlowBean(sum_upFlow,sum_downFlow);

		context.write(key,result);
	}
}
