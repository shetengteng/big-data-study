package com.stt.demo.Ch03_RandomNumber;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 随机数打印
 */
public class RandomNumberSource extends AbstractSource implements EventDrivenSource,Configurable{

	int numSize = 0;
	final List<Event> list = new ArrayList<>();

	@Override
	public void configure(Context context) {
		numSize = context.getInteger("numSize");
	}

	@Override
	public synchronized void start() {
		try {
			for(int i =0;i<numSize;i++){
				String number = new Random().nextInt(10)+"";
				list.add(EventBuilder.withBody(number.getBytes("utf-8")));
				getChannelProcessor().processEventBatch(list);
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
}
