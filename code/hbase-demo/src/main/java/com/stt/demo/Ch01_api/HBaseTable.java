package com.stt.demo.Ch01_api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface HBaseTable {
	// 表名
	String value();

	String namespace() default "default";
}
