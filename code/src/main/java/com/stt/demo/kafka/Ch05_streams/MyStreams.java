package com.stt.demo.kafka.Ch05_streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.io.UnsupportedEncodingException;
import java.util.Properties;

public class MyStreams {

	/**
	 * 需要2个topic，一个是输入，一个是输出
	 * @param args
	 */
	public static void main(String[] args) {

		String fromTopic = "fromTopic";
		String toTopic = "toTopic";
		// 参数
		Properties prop = new Properties();
		// 定义本次应用的id
		prop.put(StreamsConfig.APPLICATION_ID_CONFIG,"myFilter");
		prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

		StreamsConfig config = new StreamsConfig(prop);

		// 构建拓扑
		TopologyBuilder topologyBuilder = new TopologyBuilder()
						.addSource("mySource",fromTopic)
						.addProcessor("myProcessor", new ProcessorSupplier<byte[],byte[]>() {
							@Override
							public Processor get() {
								return new MyProcessor();
							}
						},"mySource")
						.addSink("mySink",toTopic,"myProcessor");
		// 创建streams
		KafkaStreams kafkaStreams = new KafkaStreams(topologyBuilder,config);
		kafkaStreams.start();
	}


	static class MyProcessor implements Processor<byte[], byte[]>{

		private ProcessorContext context;

		@Override
		public void init(ProcessorContext processorContext) {
			this.context = processorContext;
		}
		@Override
		public void process(byte[] key, byte[] value) {
			try {
				String input = new String(value,"utf-8");
				input = input.replaceAll(">>>","");
				context.forward(key,input.getBytes("utf-8"));
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void punctuate(long l) {
		}

		@Override
		public void close() {
		}
	}
}