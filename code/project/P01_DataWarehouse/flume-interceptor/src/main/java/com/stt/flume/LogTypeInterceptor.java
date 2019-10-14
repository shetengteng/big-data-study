package com.stt.flume;

import org.apache.commons.codec.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class LogTypeInterceptor implements Interceptor {

	@Override
	public void initialize() {
	}

	@Override
	public Event intercept(Event event) {
		// 获取数据
		String log = new String(event.getBody(), Charsets.UTF_8);
		// 对header头部进行处理
		if(log.contains("start")){
			event.getHeaders().put("topic","topic_start");
		}else{
			event.getHeaders().put("topic","topic_event");
		}
		return event;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> re = new ArrayList<>(events.size());
		events.forEach(event -> re.add(intercept(event)));
		return re;
	}
	@Override
	public void close() {

	}

	// 必须实现Interceptor的Builder类
	public static class Builder implements Interceptor.Builder{

		@Override
		public Interceptor build() {
			return new LogTypeInterceptor();
		}

		@Override
		public void configure(Context context) {

		}
	}
}