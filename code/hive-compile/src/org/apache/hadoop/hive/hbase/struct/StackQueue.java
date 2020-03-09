package org.apache.hadoop.hive.hbase.struct;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class StackQueue {

	private HashMap<String,String> map = new HashMap();

	public String encode(String longUrl){

		for (Map.Entry<String, String> entry : map.entrySet()) {
			if(entry.getValue().equals(longUrl)){
				return entry.getValue();
			}
		}
		String shortUrl = shortUrl(longUrl);
		map.put(shortUrl,longUrl);
		return shortUrl;
	}

	private String shortUrl(String longUrl) {
		String baseUrl = "http://tinyurl.com/";
		StringBuilder re = new StringBuilder(baseUrl);
		for(int i = 0;i < 6;i++){
			re.append((char)(new Random().nextInt(36)+'A'));
		}

		return re.toString();
	}

	public String decode(String shortUrl){
		if (map.containsKey(shortUrl)) {
			return map.get(shortUrl);
		}
		return "http://unknown";
	}

	public static void main(String[] args) {

		StackQueue s = new StackQueue();
		String encode = s.encode("https://leetcode.com/problems/design-tinyurl");
		System.out.println(encode);
		System.out.println(s.decode(encode));

	}


}
