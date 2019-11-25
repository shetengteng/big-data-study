package com.stt.spark.dw;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;

public class CanalClient {

	public static void main(String[] args) {
		startConnect();
	}

	public static void startConnect() {
		//destinationcanal serverinstanceexample
		CanalConnector canalConnector = CanalConnectors.newSingleConnector(
				new InetSocketAddress("hadoop102", 11111),
				"example", "", "");
		while (true) {
			canalConnector.connect();
			canalConnector.subscribe("gmall_canal.order_info");   // 访问指定数据信息，如果是全部表使用 * 表示

			// 100 条语句，可能包含1w条数据的变化
			// 一个message表示一次数据的抓取
			Message message = canalConnector.get(100);
			if (message.getEntries().size() == 0) {
				try {
					System.out.println("5s");
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				continue;
			}

			for (CanalEntry.Entry entry : message.getEntries()) {
				// 忽略事务处理消息
				if (entry.getEntryType().equals(CanalEntry.EntryType.TRANSACTIONBEGIN) ||
						entry.getEntryType().equals(CanalEntry.EntryType.TRANSACTIONEND)) {
					continue;
				}
				CanalEntry.RowChange rowChange = null;
				try {
					// 将存储的二进制值解析为内存对象
					rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
					EventHandler.handleEvent(
							entry.getHeader().getTableName(),
							rowChange.getEventType(),
							rowChange.getRowDatasList());
				} catch (InvalidProtocolBufferException e) {
					e.printStackTrace();
				}

			}
		}
	}



}