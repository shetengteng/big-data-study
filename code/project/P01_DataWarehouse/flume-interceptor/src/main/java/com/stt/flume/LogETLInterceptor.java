package com.stt.flume;

import org.apache.commons.codec.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class LogETLInterceptor implements Interceptor {

	@Override
	public void initialize() {
	}

	@Override
	public Event intercept(Event event) {
		// 获取数据
		String log = new String(event.getBody(), Charsets.UTF_8);
		// 判断数据类型进行判断是否合法
		if(log.contains("start")){
			if(validateStart(log.trim())){
				return event;
			}
		}else{
			if(validateEvent(log.trim())){
				return event;
			}
		}

		return null;
	}

	private boolean validateStart(String log) {
//		{"action":"1","ar":"MX","ba":"HTC","detail":"542","en":"start","entry":"2","extend1":"","g":"S3HQ7LKM@gmail.com","hw":"640*960","l":"en","la":"-43.4","ln":"-98.3","loading_time":"10","md":"HTC-5","mid":"993","nw":"WIFI","open_ad_type":"1","os":"8.2.1","sr":"D","sv":"V2.9.0","t":"1559551922019","uid":"993","vc":"0","vn":"1.1.5"}

		if(StringUtils.isBlank(log)){
			return false;
		}
		return validateJson(log);
	}

	private boolean validateEvent(String log) {
		// 服务器时间 | json
		// 1549696569054 | {"cm":{"ln":"-89.2","sv":"V2.0.4","os":"8.2.0","g":"M67B4QYU@gmail.com","nw":"4G","l":"en","vc":"18","hw":"1080*1920","ar":"MX","uid":"u8678","t":"1549679122062","la":"-27.4","md":"sumsung-12","vn":"1.1.3","ba":"Sumsung","sr":"Y"},"ap":"weather","et":[]}

		String[] split = log.split("\\|");

		if(split.length != 2){
			return false;
		}
		// 时间戳非法
		if(split[0].length()!=13 || !NumberUtils.isDigits(split[0])){
			return false;
		}
		return validateJson(split[1].trim());
	}

	private boolean validateJson(String json){
		// 说明不是json格式
		if(!json.startsWith("{")||!json.endsWith("}")){
			return false;
		}
		return true;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> re = new ArrayList<>(events.size());
		for (Event event : events) {
			Event intercept = intercept(event);
			if(!Objects.isNull(intercept)){
				re.add(intercept);
			}
		}
		return re;
	}

	@Override
	public void close() {
	}

	// 必须实现Interceptor的Builder类
	public static class Builder implements Interceptor.Builder{

		@Override
		public Interceptor build() {
			return new LogETLInterceptor();
		}

		@Override
		public void configure(Context context) {

		}
	}
}