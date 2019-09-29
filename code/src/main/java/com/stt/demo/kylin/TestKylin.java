package com.stt.demo.kylin;

import java.sql.*;

public class TestKylin {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {

		String driver = "org.apache.kylin.jdbc.Driver";
		String url = "jdbc:kylin://hadoop102:7070/myProject";
		String username = "ADMIN";
		String password = "KYLIN";
		Class.forName(driver);
		// 获取连接
		Connection connection = DriverManager.getConnection(url,username,password);
		// 预编译sql
		String sql = "SELECT SUM(sal) FROM emp GROUP BY deptno";
		PreparedStatement ps = connection.prepareStatement(sql);
		// 执行查询
		ResultSet resultSet = ps.executeQuery();
		// 打印结果
		while(resultSet.next()){
			// 下标从1开始
			System.out.println(resultSet.getDouble(1));
		}

		// 关闭资源
		resultSet.close();
		ps.close();
		connection.close();

	}

}
