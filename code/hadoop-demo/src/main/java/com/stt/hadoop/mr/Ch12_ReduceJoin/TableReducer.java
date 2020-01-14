package com.stt.hadoop.mr.Ch12_ReduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class TableReducer extends Reducer<Text,TableBean,TableBean,NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

		List<TableBean> orders = new ArrayList<>();
		TableBean pdBean = null;

		// 需要注意此处迭代器源码
		// 这里使用深拷贝的原因
		//      由于此处values的迭代器是Hadoop改写的，每次val都是最新的值，而非不同的引用，为了节省内存空间
		//      因此如果直接orders.add(val); 那么会导致orders里面的所有bean都是同一个对象（最后一个对象）
		for(TableBean val : values){
			if("order".equals(val.getFlag())){
				orders.add(copy(val));
			}else if("pd".equals(val.getFlag())){
				pdBean = copy(val);
			}
		}
		for(TableBean val : orders){
			if(pdBean != null){
				val.setPName(pdBean.getPName());
			}
			context.write(val,NullWritable.get());
		}
	}

	private TableBean copy(TableBean source){
		TableBean tmp = new TableBean();
		try {
			// 最好不要使用spring框架的提供的方法
			// 因为会打包普通的jar，而hadoop里面可能没有spring的类库
			// 深度拷贝
			BeanUtils.copyProperties(tmp,source);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return tmp;
	}
}
