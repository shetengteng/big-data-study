package com.stt.spark.dw.espublisher;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
public class PublishController {

	@Autowired
	PublishService publishService;

	@GetMapping("/realtime-total")
	public List getTotal(String date) throws IOException {
		long dauTotal = publishService.getDAUTotal(date);
		JSONObject json = new JSONObject()
				.fluentPut("id","dau")
				.fluentPut("name","新增日活")
				.fluentPut("value",dauTotal);
		return Lists.newArrayList(json);
	}

	@GetMapping("/realtime-hour")
	public JSONObject getLogHour(String id,String date) throws IOException {
		return new JSONObject()
				.fluentPut("today",publishService.getDAUHourCount(date))
				.fluentPut("yesterday",publishService.getDAUHourCountYesterday(date));
	}

	@GetMapping("/realtime-amount")
	public Double getTotalAmount(String date) throws IOException {
		return publishService.getOrderAmountByDate(date);
	}

	@GetMapping("/realtime-amount-hour")
	public JSONObject getTotalAmountHour(String date) throws IOException {
		return new JSONObject()
				.fluentPut("today",publishService.getOrderAmountByHour(date))
				.fluentPut("yesterday",publishService.getOrderAmountByHourYesterday(date));
	}

	@GetMapping("sale_detail")
	public JSONObject getSaleDetail(@RequestParam("date")  String date, @RequestParam("keyword") String keyword, @RequestParam("startpage") int startpage, @RequestParam("size")int size) throws IOException {

		JSONObject re = publishService.getSaleDetail(date, keyword, startpage, size);

		long total = re.getLong("total");
		long ageLt20=0;
		long age20_30=0;
		long ageGte30=0;
		for (Map.Entry<String, Object> item : re.getJSONObject("groupby_user_age").entrySet()) {
			Integer age = Integer.parseInt(item.getKey());
			if (age < 20) {
				ageLt20 += Integer.parseInt(item.getValue().toString());
			} else if (age >= 20 && age < 30) {
				age20_30 += Integer.parseInt(item.getValue().toString());
			} else {
				ageGte30 += Integer.parseInt(item.getValue().toString());
			}
		}

		long maleCount=0;
		long femaleCount=0;
		for (Map.Entry<String, Object> item : re.getJSONObject("groupby_user_gender").entrySet()) {
			String gender =  item.getKey() ;
			if ( gender.equals("M")){
				maleCount+=Integer.parseInt(item.getValue().toString());
			}else  {
				femaleCount+=Integer.parseInt(item.getValue().toString());
			}
		}

		Stat ageStat=new Stat("用户年龄占比",Lists.newArrayList(
				new Stat.Option("20岁以下",Math.round(1000D* ageLt20/ total )/10.0D),
				new Stat.Option("20岁到30岁",Math.round(1000D* age20_30/ total )/10.0D),
				new Stat.Option("30岁及30岁以上",Math.round(1000D* ageGte30/ total )/10.0D)
		));

		Stat genderStat = new Stat("用户性别占比",Lists.newArrayList(
				new Stat.Option("男",Math.round(1000D* maleCount/ total )/10.0D),
				new Stat.Option("女",Math.round(1000D* femaleCount/ total )/10.0D)
		));

		return re.fluentRemove("groupby_user_age")
				.fluentRemove("groupby_user_gender")
				.fluentPut("stat",Lists.newArrayList(ageStat,genderStat));
	}
}