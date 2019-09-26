package com.stt.demo.hbase.Ch03_guli_weibo;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.stt.demo.hbase.Ch01_api.BaseApi;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.stt.demo.hbase.Ch01_api.BaseApi.getConnection;

public class WeiboComponent {

	// 创建命名空间名称
	private static final String NAMESPACE = "Weibo";
	// 微博内容表，表名
	private static final String TABLE_CONTENT = "Weibo:content";
	// 用户关系表
	private static final String TABLE_RELATIONS = "Weibo:relations";
	// 微博收件箱表
	private static final String TABLE_RECEIVE_CONTENT = "Weibo:receive_content";

	public void initNamespace(){
		try {
			// 创建之前已经判断
			BaseApi.createNamespace(
					NAMESPACE,
					ImmutableMap.of("creator","stt",
							"create_time",System.currentTimeMillis()+"")
			);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}finally {
			try {
				BaseApi.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 创建微博内容表
	 */
	public void createTableContent(){
		try {
			BaseApi.createTable(TABLE_CONTENT,1,1,"info");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}finally {
			try {
				BaseApi.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 创建用户关系表
	 */
	public void createTableRelations(){
		try {
			BaseApi.createTable(TABLE_RELATIONS,1,1,"attends","fans");
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				BaseApi.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 创建微博收件箱表
	 */
	public void createTableReceiveContent(){
		try {
			BaseApi.createTable(TABLE_RECEIVE_CONTENT,1000,1000,"info");
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				BaseApi.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void publishMessage(String uid,String content){
		try {
			// 增加A微博的数据
			// 由于hbase的rowkey是升序，此时需要按照时间戳降序，使用Long.MAX_VALUE进行减法操作
			String rowKey = uid+"_"+(Long.MAX_VALUE-System.currentTimeMillis());
			BaseApi.addOrUpdateData(TABLE_CONTENT,rowKey,"info","content",content);

			// 获取A的所有fans数据
			List<String> fanIds = getFanIds(uid);

			// 向A的所有fans的收件箱增加数据,增加的是内容的rowKey
			if(!CollectionUtils.isEmpty(fanIds)){
				putContent(fanIds,uid,rowKey);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				BaseApi.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public List<String> getFanIds(String userId){
		List<String> ids = new ArrayList<>();
		try {
			Table table = getConnection().getTable(TableName.valueOf(TABLE_RELATIONS));
			Get get = new Get(Bytes.toBytes(userId))
					.addFamily(Bytes.toBytes("fans"));
			Result result = table.get(get);
			for (Cell cell : result.rawCells()) {
				ids.add(Bytes.toString(CellUtil.cloneQualifier(cell)));
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ids;
	}


	public void putContent(List<String> fanIds,String uid, String uid_ts){
		try {
			Table table = getConnection().getTable(TableName.valueOf(TABLE_RECEIVE_CONTENT));
			List<Put> puts = new ArrayList<>();
			for (String fanId : fanIds) {
				Put put = new Put(Bytes.toBytes(fanId))
						.addColumn(Bytes.toBytes("info"),Bytes.toBytes(uid),Bytes.toBytes(uid_ts));
				puts.add(put);
			}
			table.put(puts);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 关注好友
	 * @param uid
	 * @param attends
	 */
	public void attendUser(String uid,String attends){
		try {
			// 对当前主动操作的用户添加新的关注的好友
			BaseApi.addOrUpdateData(TABLE_RELATIONS,uid,"attends",attends,"1");
			// 微博用户关系表中，对被关注的用户添加粉丝（当前操作的用户）
			BaseApi.addOrUpdateData(TABLE_RELATIONS,attends,"fans",uid,"1");
			// 查询 attends发送的数据
			List<String> weiboRowkey = scanWeibo(attends);

			if(!CollectionUtils.isEmpty(weiboRowkey)){
				// 当前操作用户的微博收件箱添加所关注的用户发布的微博rowkey
				Table table = getConnection().getTable(TableName.valueOf(TABLE_RECEIVE_CONTENT));
				List<Put> puts = new ArrayList<>();
				for (String val : weiboRowkey) {
					// 注意相同的family，相同的column，赋值多个value，需要指定不同的timestamp
					Long ts = Long.MAX_VALUE - Long.valueOf(val.split("_")[1]);
					Put put = new Put(Bytes.toBytes(uid))
							.addColumn(Bytes.toBytes("info"),Bytes.toBytes(attends),ts,Bytes.toBytes(val));
					puts.add(put);
				}
				if(!CollectionUtils.isEmpty(puts)){
					// 增加数据
					table.put(puts);
				}
				// 关闭表格
				table.close();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				BaseApi.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 查询最近的数据
	 * @param attends
	 * @return
	 */
	private List<String> scanWeibo(String attends) {
		// 获取5条
		List<String> content_id = new ArrayList<>(5);
		try {
			Table table = getConnection().getTable(TableName.valueOf(TABLE_CONTENT));
			Scan scan = new Scan();
			// attends_ < xxxx < attends_|
			// | 在ascii中，第二大符号
			scan.setStartRow(Bytes.toBytes(attends+"_"));
			scan.setStopRow(Bytes.toBytes(attends+"_|"));
			ResultScanner scanner = table.getScanner(scan);
			for (Result result : scanner) {
				if(content_id.size() >= 5){
					break;
				}
				for (Cell cell : result.rawCells()) {
					// 获取内容的rowKey
					content_id.add(Bytes.toString(CellUtil.cloneRow(cell)));
				}
			}
			scanner.close();
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return content_id;
	}

	// 获取关注人的微博内容
	public List<Message> getAttendsContent(String uid) {
		List<Message> re = new ArrayList<>();
		try {
			//获取收件箱中uid关注的信息
			List<String> contentRowKeys = BaseApi.getRowDataList(TABLE_RECEIVE_CONTENT,uid,5);

			// 通过内容的rowKey查询微博内容表获取相关结果
			Table table = getConnection().getTable(TableName.valueOf(TABLE_CONTENT));
			List<Get> gets = new ArrayList<>();
			contentRowKeys.forEach(k->gets.add(new Get(Bytes.toBytes(k))));
			Result[] results = table.get(gets);
			for (Result result : results) {
				if(!result.isEmpty()){
					for (Cell cell : result.rawCells()) {
						// 结果封装
						String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
						re.add(Message.builder()
								.uid(rowKey.substring(0,rowKey.indexOf("_")))
								.timestamp(rowKey.substring(rowKey.indexOf("_")+1))
								.content(Bytes.toString(CellUtil.cloneValue(cell)))
								.build());
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				BaseApi.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return re;
	}


	/**
	 * 取消关注
	 */
	public void removeAttends(String uid,String ... attends){

		try {
			// 删除uid的关注信息
			BaseApi.deleteData(TABLE_RELATIONS,uid,"attends",attends);

			// 删除attends的fans中的uid信息
			Table table = getConnection().getTable(TableName.valueOf(TABLE_RELATIONS));
			List<Delete> deletes = Lists.newArrayList();
			for (String attend : attends) {
				deletes.add(new Delete(Bytes.toBytes(attend)).addColumn(Bytes.toBytes("fans"),Bytes.toBytes(uid)));
			}
			table.delete(deletes);
			table.close();

			// 从收件箱中删除相应的微博记录
			BaseApi.deleteData(TABLE_RECEIVE_CONTENT,uid,"info",attends);

		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				BaseApi.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}