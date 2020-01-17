package com.stt.demo.Ch03_RandomNumber;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;


public class ConsoleSink extends AbstractSink implements Configurable {

	@Override
	public void configure(Context context) {

	}
	@Override
	public Status process() throws EventDeliveryException {
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction tx = channel.getTransaction();
		Event event = null;

		try {
			tx.begin();
			event = channel.take();
			if(event != null){
				System.out.println("event "+new String(event.getBody(),"utf-8"));
			}else{
				result = Status.BACKOFF;
			}
			tx.commit();
		}catch (Exception ex){
			tx.rollback();
			throw new EventDeliveryException("fail to log event"+event,ex);
		}finally {
			tx.close();
		}
		return result;
	}
}