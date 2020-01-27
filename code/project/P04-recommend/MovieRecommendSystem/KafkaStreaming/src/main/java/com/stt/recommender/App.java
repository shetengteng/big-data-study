package com.stt.recommender;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.HashMap;


public class App {

	public static void main(String[] args) {

		String brokers = "hadoop102:9092";
		String zookeepers = "hadoop102:2181";

		String fromTopic = "log";
		String toTopic = "recommender";

		StreamsConfig config = new StreamsConfig(
				new HashMap<String,String>(3){{
					put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
					put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
					put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);
				}}
		);

		// 拓扑构建器
		new KafkaStreams(
				new TopologyBuilder()
					.addSource("SOURCE",fromTopic)
					.addProcessor("PROCESS",()-> new LogProcessor(),"SOURCE")
					.addSink("SINK",toTopic,"PROCESS")
				,config
		).start();

	}

	// 编写 LogProcessor
	private static class LogProcessor implements Processor<byte[],byte[]>{

		private ProcessorContext context;

		@Override
		public void init(ProcessorContext processorContext) {
			context = processorContext;
		}

		@Override
		public void process(byte[] key, byte[] line) {

			String input = new String(line);
			// key 暂时没有用到
			System.out.println(new String(key)+"---"+input);

			// 根据前缀过滤日志信息，提取后面的内容
			if(input.contains("MOVIE_RATING_PREFIX:")){
				System.out.println("movie ratting start .."+input);
				input = input.split("MOVIE_RATING_PREFIX:")[1].trim();
				context.forward("logProcessor".getBytes(),input.getBytes());
			}
		}

		@Override
		public void punctuate(long l) { }

		@Override
		public void close() { }
	}
}