package com.stt.demo.hive.Ch02_ETL;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

	Text k = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String etlStr = oriStr2ETLStr(value.toString());
		if(StringUtils.isBlank(etlStr)){
			return;
		}
		k.set(etlStr);
		context.write(k,NullWritable.get());
	}

	public static String oriStr2ETLStr(String s) {
		StringBuilder sb = new StringBuilder();
		String[] fields = s.split("\t");
		int len = fields.length;
		// 清洗不满足的数据
		if(len < 9){
			return null;
		}
		// 对第4个字段中的 & 两边的空格进行处理
		fields[3] = fields[3].replace(" ","");
		// 拼接操作
		for(int i=0;i<len;i++){
			if(i<9){
				sb.append(fields[i]);
				// related id可以为空，最后一个拼接不能有\t
				if(i != len-1){
					sb.append("\t");
				}
			}else{
				// related id 不为空，需要用&进行连接
				sb.append(fields[i]);
				if(i!=len-1){
					sb.append("&");
				}
			}
		}
		return sb.toString();
	}

	public static void main(String[] args) {
		System.out.println(oriStr2ETLStr("LKh7zAJ4nwo\tTheReceptionist\t653\tEntertainment\t424\t13021\t4.34\t1305\t744\tDjdA-5oKYFQ\tNxTDlnOuybo\tc-8VuICzXtU\tDH56yrIO5nI\tW1Uo5DQTtzc\tE-3zXq_r4w0\t1TCeoRPg5dE\tyAr26YhuYNY\t2ZgXx72XmoE\t-7ClGo-YgZ0\tvmdPOOd6cxI\tKRHfMQqSHpk"));
		System.out.println(oriStr2ETLStr("LKh7zAJ4nwo\tTheReceptionist\t653\tEntertainment\t424\t13021\t4.34"));
	}
}
