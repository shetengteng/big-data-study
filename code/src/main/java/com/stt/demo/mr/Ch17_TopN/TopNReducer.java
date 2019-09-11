package com.stt.demo.mr.Ch17_TopN;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.TreeMap;

public class TopNReducer extends Reducer<Text,FlowBean,Text,FlowBean> {

	TreeMap<FlowBean,Text> treeMap = new TreeMap<>();

	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
		// 对分区数据进行汇总过滤
		for(FlowBean val : values){
			try {
				FlowBean flowBean = new FlowBean();
				// 这里需要注意，val始终是同一个引用
				BeanUtils.copyProperties(flowBean,val);
				treeMap.put(flowBean,key);
				if(treeMap.size() > 10){
					treeMap.pollLastEntry();
				}
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for(Map.Entry<FlowBean,Text> entry : treeMap.entrySet()){
			context.write(entry.getValue(),entry.getKey());
		}
		// 释放资源
		treeMap.clear();
	}
}