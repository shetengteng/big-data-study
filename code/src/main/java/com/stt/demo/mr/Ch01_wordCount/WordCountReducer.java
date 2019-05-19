package com.stt.demo.mr.Ch01_wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 输入的是单词text和1
 * 输出的是单词text和具体的个数
 * Created by Administrator on 2019/5/5.
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {


	int sum = 0;
	IntWritable v = new IntWritable();

	// 每次会获取一个key，value的list作为输入
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		sum = 0;
		for(IntWritable count : values){
			sum += count.get();
		}
		// 输出
		v.set(sum);
		context.write(key,v);
	}
}
