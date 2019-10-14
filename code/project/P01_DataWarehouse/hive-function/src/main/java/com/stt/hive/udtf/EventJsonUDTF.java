package com.stt.hive.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF {

	// 一进多出
	// 该方法中，我们将指定输出参数的名称和参数类型：
	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
		List<String> fieldName = new ArrayList<>(2);
		List<ObjectInspector> fieldType = new ArrayList<>(2);
		fieldName.add("event_name");
		fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldName.add("event_json");
		fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName,fieldType);
	}

	@Override
	public void process(Object[] objects) throws HiveException {
		// 获取输入数据
		String input = objects[0].toString();
		if(StringUtils.isBlank(input)){
			return;
		}
		try {
			// 获取一共有几个事件（ad/facoriters）
			JSONArray ja = new JSONArray(input);
			if(ja == null){
				return;
			}
			// 循环遍历每一个事件
			for(int i=0;i<ja.length();i++){
				try {
					String[] results = new String[2];
					// 取出每个的事件名称（ad/facoriters）
					results[0] = ja.getJSONObject(i).getString("en");
					// 取出每一个事件整体
					results[1] = ja.getString(i);
					// 将结果返回，和mr中的content的write是一致的
					forward(results);
				} catch (JSONException e) {
					e.printStackTrace();

				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	//当没有记录处理的时候该方法会被调用，用来清理代码或者产生额外的输出
	@Override
	public void close() throws HiveException {
	}
}