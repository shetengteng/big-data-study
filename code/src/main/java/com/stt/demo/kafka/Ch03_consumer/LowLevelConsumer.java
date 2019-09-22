package com.stt.demo.kafka.Ch03_consumer;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LowLevelConsumer {

	/**
	 * 读取指定topic，指定partition,指定offset的数据
	 * 因为一个topic中存在多个partition，而且每一个partition中会有多个副本，所以想要获取指定的数据
	 * 必须从指定分区的主副本中获取数据，那么就必须拿到主副本的元数据信息。
	 * 1）发送主题元数据请求对象
	 * 2）得到主题元数据响应对象，其中包含主题元数据信息
	 * 3）通过主题元数据信息，获取主题的分区元数据信息
	 * 4）获取指定的分区元数据信息
	 * 5）获取分区的主副本信息
	 *
	 * 获取主副本信息后，消费者要连接对应的主副本机器，然后抓取数据
	 * 1）构建抓取数据请求对象
	 * 2）发送抓取数据请求
	 * 3）获取抓取数据响应，其中包含了获取的数据
	 * 4）由于获取的数据为字节码，还需要转换为字符串，用于业务的操作。
	 * 5）将获取的多条数据封装为集合。
 	 * @param args
	 */
	public static void main(String[] args) throws UnsupportedEncodingException {
		List<String> brokersHost = Arrays.asList("hadoop102", "hadoop103", "hadoop104");
		// 连接kafka的集群端口号
		int port = 9092;
		String topic = "api_test";
		int partition = 0;
		long offset = 0;
		// 获取指定topic指定分区的leader的ip
		String leaderHost = getLeaderHost(brokersHost,port,topic,partition);
		// 获取数据
		List<String> data = getData(leaderHost, port, topic, partition, offset);
		for (String datum : data) {
			System.out.println(datum);
		}
	}

	private static String getLeaderHost(List<String> brokersHost, int port, String topic, int partition) {
		for (String broker : brokersHost) {
			// 遍历集群，根据节点信息创建SimpleConsumer
			// 超时时间 ms单位
			// 最后参数：客户端id，用于区分SimpleConsumer
			SimpleConsumer getLeader = new SimpleConsumer(broker, port, 1000, 1024 * 4, "getLeader");
			// 根据传入的主题信息创建元数据请求
			TopicMetadataRequest request = new TopicMetadataRequest(Arrays.asList(topic));
			// 发送元数据请求得到返回值
			TopicMetadataResponse response = getLeader.send(request);

			List<TopicMetadata> topicMetadata = response.topicsMetadata();
			for (TopicMetadata topicMetadatum : topicMetadata) {
				//一个Topic由多个Partition组成
				List<PartitionMetadata> partitionMetadata = topicMetadatum.partitionsMetadata();
				// 遍历多个分区的元数据信息
				for (PartitionMetadata partitionMetadatum : partitionMetadata) {
					// 匹配传入的分区号
					if(partitionMetadatum.partitionId() == partition){
						return partitionMetadatum.leader().host();
					}
				}
			}

		}
		return null;
	}

	private static List<String> getData(String leaderHost, int port, String topic, int partition, long offset) throws UnsupportedEncodingException {
		SimpleConsumer consumer = new SimpleConsumer(leaderHost,port,1000,1024*1024,"getData");
		FetchRequest request = new FetchRequestBuilder().addFetch(topic,partition,offset,1024*1024).build();
		FetchResponse response = consumer.fetch(request);
		ByteBufferMessageSet messageAndOffsets = response.messageSet(topic, partition);
		List<String> result = new ArrayList<>();
		for (MessageAndOffset m : messageAndOffsets) {
//			long nowOffset = m.offset();
			ByteBuffer payload = m.message().payload();
			byte[] bytes = new byte[payload.limit()];
			payload.get(bytes);
			String val = new String(bytes,"utf-8");
			result.add(val);
		}
		return result;
	}

}