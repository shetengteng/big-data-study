package com.stt.hadoop.mr.Ch01_WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN:输入数据Key
 * VALUEIN:输入数据value
 * KEYOUT:输出数据key
 * VALUEOUT：输出数据value
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{


	// 这里使用属性变量的意义在于节省内存
	// 定义输出的key对象
	Text k = new Text();
	// 定义输出的值,值都是1,匹配到一个单词就放入context中
	IntWritable v = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 读取一行数据
		String line = value.toString();
		// 对这一行进行空格分隔
		String[] words = line.split("\\s+");
		// 输出
		for(String word : words){
			k.set(word);
			context.write(k,v);
		}
	}
}
