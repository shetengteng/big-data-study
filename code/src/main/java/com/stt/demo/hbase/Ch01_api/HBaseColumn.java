package com.stt.demo.hbase.Ch01_api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface HBaseColumn {
	String family() default "default";
	// 为空则使用原先的属性名
	String column() default "";

}
