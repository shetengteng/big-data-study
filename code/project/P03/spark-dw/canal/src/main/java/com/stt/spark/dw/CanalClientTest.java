package com.stt.spark.dw;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.util.CollectionUtils;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClientTest {
	public static void main(String[] args) {

		// 创建于cannal服务器的连接
		CanalConnector canalConnector = CanalConnectors
				.newSingleConnector(
						new InetSocketAddress("hadoop102",11111),
						"example","",""
				);
		// 不断拉取数据
		while (true){
			canalConnector.connect();
			// 订阅表
			canalConnector.subscribe("gmall_canal.order_info");
			// 连接，抓取100条操作记录
			Message msg = canalConnector.get(100);
			if(msg.getEntries().size() == 0){
				try{
					System.out.println("sleep...");
					Thread.sleep(5000);
				}catch (Exception e){
					e.printStackTrace();
				}
				continue;
			}

			for (CanalEntry.Entry entry : msg.getEntries()) {
				// 需要entry的storeValue（序列化的内容）
				try {
					// 只要行变化内容
					if(entry.getEntryType() != CanalEntry.EntryType.ROWDATA){
						continue;
					}
					CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());

					// 表名
					String tableName = entry.getHeader().getTableName();
					// 操作类型
					CanalEntry.EventType eventType = rowChange.getEventType();
					// 行集
					List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

					if("order_info".equals(tableName)
							&& CanalEntry.EventType.INSERT.equals(eventType)
							&& !CollectionUtils.isEmpty(rowDatasList)
							){
						for (CanalEntry.RowData rowData : rowDatasList) {
							// 修改后的列集合
							List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
							for (CanalEntry.Column column : afterColumnsList) {
								System.out.println(column.getName()+"="+column.getValue());
							}
						}
					}

				} catch (InvalidProtocolBufferException e) {
					e.printStackTrace();
				}
			}
		}

	}
}
