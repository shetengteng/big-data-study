package com.stt.kafka.Ch04_interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;


public class InterceptorProducer {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "hadoop102:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// 构建拦截链
		props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Arrays.asList(
				"com.stt.demo.kafka.Ch04_interceptor.TimeInterceptor",
				"com.stt.demo.kafka.Ch04_interceptor.CounterInterceptor"));

		Producer<String, String> producer = new KafkaProducer<>(props);
		// 3 发送消息
		for (int i = 0; i < 10; i++) {
			producer.send(new ProducerRecord<>("api_test", "message" + i));
		}

		//一定要关闭producer，这样才会调用interceptor的close方法
		producer.close();
	}
}