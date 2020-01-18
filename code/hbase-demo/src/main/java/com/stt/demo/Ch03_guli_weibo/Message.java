package com.stt.demo.Ch03_guli_weibo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Message {
	private String uid;
	private String content;
	private String timestamp;
}
