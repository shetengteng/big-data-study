package com.stt.demo.hbase.Ch01_api;

import lombok.Builder;
import lombok.Data;


@Data
@Builder
@HBaseTable(value = "user",namespace = "test")
public class Student {

	@HRowKey
	private String rowKey;
	@HBaseColumn(family = "info")
	private String name;
	@HBaseColumn(family = "info",column = "addr")
	private String address;
	private String email;
	private String age;
	private String tel;

}
