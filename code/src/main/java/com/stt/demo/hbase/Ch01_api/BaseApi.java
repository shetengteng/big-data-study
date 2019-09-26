package com.stt.demo.hbase.Ch01_api;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;
import scala.Int;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BaseApi {

	// 获取配置对象
	public static Configuration config = HBaseConfiguration.create();
	public static ThreadLocal<Admin> adminThreadLocal = new ThreadLocal<>();
	public static ThreadLocal<Connection> connThreadLocal = new ThreadLocal<>();

	static {
		// 若在resources中没有hbase-site.xml配置,则需要添加如下
//		config.set("hbase.zookeeper.quorum","hadoop102");
//		config.set("hbase.zookeeper.property.clientPort","2181");
	}

	public static void main(String[] args) throws Exception {
		String tableName = "student";
//		boolean isExists = isTableExists("student");
//		boolean isExists2 = isTableExists("test:user");
//		System.out.println(isExists);
//		System.out.println(isExists2);

//		createTable("test:createUser","info");

//		dropTable("test:createUser");

//		createNamespace("myNamespace");

//		addOrUpdateData("test:user","1004","info","name","stt");
//		addOrUpdateData("test:user","1004","info","age","11");

//		deleteData("test:user","1004","info","name");
//		deleteData("test:user","1004","info","age");
//		deleteRowData("test:user","1003");

//		getAllData("test:user");
//		getRowData("test:user","1002");
//		getRowQualifier("test:user","1002","info","name");

		addObject(Student.builder().rowKey("1005").address("addr2").age("11").name("ceshi").build());


		close();
	}

	public static Connection getConnection() throws IOException {
		Connection connection = connThreadLocal.get();
		if(connection == null){
			// 建立连接
			connection = ConnectionFactory.createConnection(config);
			connThreadLocal.set(connection);
		}
		return connection;
	}

	public static Admin getAdmin() throws IOException {
		Admin admin = adminThreadLocal.get();
		if (admin == null){
			// 建立连接,过期的
			// HBaseAdmin admin = new HBaseAdmin(config);
			// 获取admin对象
			admin = getConnection().getAdmin();
			adminThreadLocal.set(admin);
		}
		return admin;
	}

	public static void close() throws IOException {
		Admin admin = adminThreadLocal.get();
		if(admin != null){
			admin.close();
			adminThreadLocal.remove();
			connThreadLocal.remove();
		}
	}

	// 通过异常判断命名空间是否存在
	public static boolean isNamespaceExists(String namespaceName){
		try{
			getAdmin().getNamespaceDescriptor(namespaceName);
			return true;
		} catch (NamespaceNotFoundException e) {
			return false;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static void createNamespace(String namespaceStr,Map<String,String> confg) throws IOException {
		if(isNamespaceExists(namespaceStr)){
			System.out.println("命名空间已存在");
			return;
		}
		NamespaceDescriptor build = NamespaceDescriptor
				.create(namespaceStr)
				.build();
		if(confg != null && confg.size() != 0){
			confg.forEach((k,v) -> build.setConfiguration(k,v));
		}
		getAdmin().createNamespace(build);
	}

	public static void createNamespace(String namespaceStr) throws IOException {
		createNamespace(namespaceStr,null);
	}

	public static boolean isTableExists(String tableName) throws IOException {
		Admin admin = getAdmin();
		return admin.tableExists(TableName.valueOf(tableName));
	}

	public static void createTable(String tableName,String columnFamily,Integer maxVersions,Integer minVersions,String ... columnFamilies) throws IOException {

		if(isTableExists(tableName)){
			System.out.println("表已经存在");
			return;
		}

		Admin admin = getAdmin();
		// 获取表格描述器
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

		if(!StringUtils.isBlank(columnFamily)){
			// 列族描述器
			HColumnDescriptor family = new HColumnDescriptor(columnFamily);
			//设置块缓存
			family.setBlockCacheEnabled(true);
			//设置块缓存大小
			family.setBlocksize(2097152);
			//设置压缩方式
			//family.setCompressionType(Algorithm.SNAPPY);
			//设置版本确界
			if(maxVersions != null){
				family.setMaxVersions(maxVersions);
			}
			if(minVersions != null){
				family.setMinVersions(minVersions);
			}
			// 添加列族
			hTableDescriptor.addFamily(family);
		}

		if(columnFamilies != null){
			for (String column : columnFamilies) {
				HColumnDescriptor f = new HColumnDescriptor(column)
						.setBlockCacheEnabled(true)
						.setBlocksize(2097152);

				//f.setCompressionType(Algorithm.SNAPPY);

				if(maxVersions != null){
					f.setMaxVersions(maxVersions);
				}
				if(minVersions != null){
					f.setMinVersions(minVersions);
				}
				hTableDescriptor.addFamily(f);
			}
		}
		admin.createTable(hTableDescriptor);
	}

	public static void createTable(String tableName,String ... columnFamilies) throws IOException {
		createTable(tableName,null,null,null,columnFamilies);
	}

	public static void createTable(String tableName,Integer maxVersions,Integer minVersions,String ... columnFamilies) throws IOException {
		createTable(tableName,null,maxVersions,minVersions,columnFamilies);
	}

	public static void dropTable(String tableName) throws IOException {
		if(isTableExists(tableName)){
			Admin admin = getAdmin();
			// 删除表注意要先改状态
			admin.disableTable(TableName.valueOf(tableName));
			admin.deleteTable(TableName.valueOf(tableName));
		}
	}

	public static void addOrUpdateData(String tableName,String rowKey,String family,String column,String ... value) throws IOException {

		Table table = getConnection().getTable(TableName.valueOf(tableName));
		List<Put> puts = new ArrayList<>();

		Long time = System.currentTimeMillis();
		for (String val : value) {
			// 注意相同的family，相同的column，赋值多个value，需要指定不同的timestamp
			Put put = new Put(Bytes.toBytes(rowKey))
					.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),time++,Bytes.toBytes(val));
			puts.add(put);
		}
		if(!CollectionUtils.isEmpty(puts)){
			// 增加数据
			table.put(puts);
		}
		// 关闭表格
		table.close();
	}

	public static void deleteData(String tableName,String rowKey,String family,String ... columns) throws IOException {

		Table table = getConnection().getTable(TableName.valueOf(tableName));
		Delete val = new Delete(Bytes.toBytes(rowKey));

		if(!StringUtils.isBlank(family)){
			if(columns == null || columns.length == 0){
				val.addFamily(Bytes.toBytes(family));
			}else{
				for (String column : columns) {
					// 注意，使用addColumns和addColumn的区别，前者删除所有版本，后者删除最新的
					val.addColumns(Bytes.toBytes(family), Bytes.toBytes(column));
				}
			}
		}

		table.delete(val);
		table.close();
	}

	public static void deleteRowData(String tableName,String rowKey) throws IOException {
		deleteData(tableName,rowKey,null,null);
	}

	public static void deleteMultiRowData(String tableName,String ... rowKeys) throws IOException {
		Table table = getConnection().getTable(TableName.valueOf(tableName));
		List<Delete> val = new ArrayList<>();
		for (String rowKey : rowKeys) {
			val.add(new Delete(Bytes.toBytes(rowKey)));
		}
		table.delete(val);
		table.close();
	}

	public static void getAllData(String tableName) throws IOException {
		Table table = getConnection().getTable(TableName.valueOf(tableName));
		// 全表扫描
		Scan scan = new Scan();
		// 获取结果
		ResultScanner scanner = table.getScanner(scan);

		// 每次读取一批数据，不会一次全部读取
//		scanner.next();

		for (Result result : scanner) {
			Cell[] cells = result.rawCells();
			for (Cell cell : cells) {
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				String column = Bytes.toString(CellUtil.cloneQualifier(cell));
				String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
				String val = Bytes.toString(CellUtil.cloneValue(cell));
				System.out.println(family+":"+column+":"+rowKey+":"+val);
			}
		}
		scanner.close();
		table.close();
	}

	public static void getRowData(String tableName,String rowKey) throws IOException {
		Table table = getConnection().getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
//		get.setMaxVersions(); 设置显示所有版本
//		get.setTimeStamp(); 设置显示指定的时间戳版本
		Result result = table.get(get);
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			String family = Bytes.toString(CellUtil.cloneFamily(cell));
			String column = Bytes.toString(CellUtil.cloneQualifier(cell));
			String val = Bytes.toString(CellUtil.cloneValue(cell));
			System.out.println(family+":"+column+":"+rowKey+":"+val);
		}
		table.close();
	}

	public static List<String> getRowDataList(String tableName, String rowKey, Integer versions) throws IOException {
		Table table = getConnection().getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(rowKey)).setMaxVersions(versions);
//		get.setTimeStamp(); 设置显示指定的时间戳版本
		Result result = table.get(get);
		Cell[] cells = result.rawCells();
		List<String> re = new ArrayList<>();
		for (Cell cell : cells) {
			re.add(Bytes.toString(CellUtil.cloneValue(cell)));
		}
		table.close();
		return re;
	}


	public static void getRowQualifier(String tableName,String rowKey,String family,String qualifier) throws IOException {
		Table table = getConnection().getTable(TableName.valueOf(tableName));

		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier));

		Result result = table.get(get);
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			String column = Bytes.toString(CellUtil.cloneQualifier(cell));
			String val = Bytes.toString(CellUtil.cloneValue(cell));
			System.out.println(family+":"+column+":"+rowKey+":"+val);
		}
		table.close();
	}

	public static void addObject(Object object) throws IOException, IllegalAccessException {
		Class clazz = object.getClass();
		HBaseTable hBaseTableAnn = (HBaseTable)clazz.getAnnotation(HBaseTable.class);

		if(hBaseTableAnn!=null){
			Table table = getConnection().getTable(TableName.valueOf(hBaseTableAnn.namespace()+":"+hBaseTableAnn.value()));
			String rowKey = "";
			Field[] fields = clazz.getDeclaredFields();

			for (Field field : fields) {
				HRowKey rowKeyAnn = field.getAnnotation(HRowKey.class);
				if(rowKeyAnn != null){
					field.setAccessible(true);
					rowKey = (String) field.get(object);
					break;
				}
			}

			if(StringUtils.isBlank(rowKey)){
				throw new RuntimeException("rowkey is empty");
			}

			List<Put> vals = new ArrayList<>();
			for (Field field : fields) {
				HBaseColumn columnAnn = field.getAnnotation(HBaseColumn.class);
				if(columnAnn != null){
					field.setAccessible(true);
					String value = (String) field.get(object);
					String family = columnAnn.family();
					String column = columnAnn.column();
					if(StringUtils.isBlank(column)){
						column = field.getName();
					}
					Put val = new Put(Bytes.toBytes(rowKey))
							.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value));
					vals.add(val);
				}
			}

			if(!CollectionUtils.isEmpty(vals)){
				// 增加数据
				table.put(vals);
			}
			// 关闭表格
			table.close();
		}
	}
}