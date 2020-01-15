package com.stt.hive.Ch01_udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Objects;

public class Lower extends UDF {

	public String evaluate (final String s) {
		return Objects.isNull(s) ? null : s.toLowerCase();
	}
}