package com.stt.demo.Ch01.part01_simulation_scala;

public class TestScala {

	public static void main(String[] args) {
		TestScala$.MODULE$.main(args);
	}

}

final class TestScala${
	// MODULE$ 是一个单例的
	public static final TestScala$ MODULE$;
	static {
		MODULE$ = new TestScala$();
	}

	public void main(String[] args){
		System.out.println("hello scala");
	}

}
