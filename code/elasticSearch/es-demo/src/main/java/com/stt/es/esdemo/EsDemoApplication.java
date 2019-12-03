package com.stt.es.esdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.lang.ref.WeakReference;

@SpringBootApplication
public class EsDemoApplication {

//	public static void main(String[] args) {
//		SpringApplication.run(EsDemoApplication.class, args);
//	}


	public static void main(String[] args) {
		Car car = new Car(2000.0, "red");
		WeakReference<Car> wrc = new WeakReference<>(car);
		int i = 0;
		while (true)
		{
			if (wrc.get() != null)
			{
				i++;
				System.out.println("WeakReferenceCar's Car is alive for " + i + ", loop - " + wrc);
			}
			else
			{
				System.out.println("WeakReferenceCar's Car has bean collected");
				break;
			}
		}
	}

	static class Car {
		private double     price;
		private String    color;

		public Car(double price, String color)
		{
			this.price = price;
			this.color = color;
		}
	}
}
