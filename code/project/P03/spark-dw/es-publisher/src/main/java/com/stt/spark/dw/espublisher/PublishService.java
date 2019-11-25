package com.stt.spark.dw.espublisher;

import com.alibaba.fastjson.JSONObject;
import com.stt.spark.dw.GmallConstant;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.stt.spark.dw.GmallConstant.ES_INDEX_DAU;
import static com.stt.spark.dw.GmallConstant.ES_INDEX_ORDER;

@Service
public class PublishService {

	@Autowired
	JestClient jestClient;

	//		GET gmall_dau/_search
//		{
//			"query" : {
//			"bool" : {
//				"filter" : {
//					"match" : {"logDate" : {"query" : "2019-02-01"}}
//				}
//			}
//		}
//		}
	public long getDAUTotal(String date) throws IOException {
		SearchSourceBuilder queryBuilder =
				new SearchSourceBuilder()
						.query(
								QueryBuilders.boolQuery()
										.filter(
												QueryBuilders.matchQuery("logDate", date)
										)
						).size(0);
		Search search = new Search
				.Builder(queryBuilder.toString())
				.addIndex(ES_INDEX_DAU)
				.addType("_doc")
				.build();

		return jestClient.execute(search).getTotal();
	}

	public long getDAUTotal2(String date) throws IOException {
		String dsl = " {\n" +
				"  \"query\" : {\n" +
				"    \"bool\" : {\n" +
				"      \"filter\" : {\n" +
				"        \"match\" : {\"logDate\" : {\"query\" : \"" + date + "\"}}\n" +
				"      }\n" +
				"    }\n" +
				"  }\n" +
				"}";

		Search search = new Search
				.Builder(dsl)
				.addIndex(ES_INDEX_DAU)
				.addType("_doc")
				.build();

		SearchResult result = jestClient.execute(search);
		return result.getTotal();

	}

	public JSONObject getDAUHourCount(String date) throws IOException {
		SearchSourceBuilder queryBuilder = new SearchSourceBuilder()
				.size(0)
				.query(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("logDate", date)))
				.aggregation(AggregationBuilders.terms("groupby_logHour").field("logHour").size(24));
		Search search = new Search
				.Builder(queryBuilder.toString())
				.addIndex(ES_INDEX_DAU)
				.addType("_doc")
				.build();

		List<TermsAggregation.Entry> groupby_logHour = jestClient.execute(search)
				.getAggregations()
				.getTermsAggregation("groupby_logHour").getBuckets();

		JSONObject re = new JSONObject();

		groupby_logHour.forEach(item -> re.put(item.getKey(), item.getCount()));

		return re;
	}

	public JSONObject getDAUHourCountYesterday(String date) throws IOException {
		String yesterday =
				LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
						.plusDays(-1).toString();
		return getDAUHourCount(yesterday);
	}

	/**
	 * 获取日期的交易额
	 *
	 * @param date
	 * @return
	 */
	public Double getOrderAmountByDate(String date) throws IOException {

		SearchSourceBuilder queryBuilder = new SearchSourceBuilder()
				.size(0)
				.query(
						QueryBuilders.boolQuery().filter(
								QueryBuilders.termQuery("createDate", date)
						)
				).aggregation(
						AggregationBuilders.sum("totalAmountByDay").field("totalAmount")
				);
		Search search = new Search.Builder(queryBuilder.toString())
				.addIndex(ES_INDEX_ORDER)
				.addType("_doc")
				.build();

		return jestClient.execute(search)
				.getAggregations()
				.getSumAggregation("totalAmountByDay")
				.getSum();

	}

	/**
	 * 分时交易额
	 * @param date
	 * @return
	 * @throws IOException
	 */
	public JSONObject getOrderAmountByHour(String date) throws IOException {

		SearchSourceBuilder queryBuilder = new SearchSourceBuilder().size(0)
				.query(
						QueryBuilders.boolQuery().filter(
								QueryBuilders.termQuery("createDate",date)
						)
				)
				.aggregation(
						AggregationBuilders
								.terms("groupby_createHour")
									.field("createHour")
									.size(24)
								.subAggregation(
										AggregationBuilders
												.sum("totalAmountByHour")
												.field("totalAmount"))
				);

		Search search = new Search.Builder(queryBuilder.toString())
						.addIndex(ES_INDEX_ORDER)
						.addType("_doc")
						.build();

		List<TermsAggregation.Entry> groupby_createHour = jestClient.execute(search)
				.getAggregations()
				.getTermsAggregation("groupby_createHour").getBuckets();

		JSONObject re = new JSONObject();

		groupby_createHour.forEach(item ->
				re.put(
						item.getKey(),
						item.getSumAggregation("totalAmountByHour").getSum()
				)
		);

		return re;

	}

	public Object getOrderAmountByHourYesterday(String date) throws IOException {
		String yesterday =
				LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
						.plusDays(-1).toString();
		return getOrderAmountByHour(yesterday);
	}


	public JSONObject getSaleDetail(String date,String keyword ,int startPage,int size) throws IOException {

		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
				.filter(QueryBuilders.termQuery("dt", date));

		if(!StringUtils.isEmpty(keyword)){
			boolQuery.must(QueryBuilders.matchQuery("sku_name",keyword).operator(Operator.AND));
		}

		SearchSourceBuilder queryBuilder = new SearchSourceBuilder()
				.query(boolQuery)
				.size(size)
				.from((startPage-1)*size)
				.aggregation(
						AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2)
				).aggregation(
						AggregationBuilders.terms("groupby_user_age").field("user_age").size(100)
				);

		System.out.println(queryBuilder.toString());

		Search search = new Search
				.Builder(queryBuilder.toString())
				.addIndex(GmallConstant.ES_INDEX_SALE)
				.addType("_doc")
				.build();

		SearchResult execute = jestClient.execute(search);

		return new JSONObject()
					.fluentPut("total",
							execute.getTotal())
					.fluentPut("detail",
							execute.getHits(Map.class)
									.stream()
									.map(hit -> hit.source)
									.collect(Collectors.toList()))
					.fluentPut("groupby_user_gender",
							execute.getAggregations()
									.getTermsAggregation("groupby_user_gender")
									.getBuckets()
									.stream()
									.collect(Collectors.toMap(TermsAggregation.Entry::getKey,TermsAggregation.Entry::getCount)))
					.fluentPut("groupby_user_age",
							execute.getAggregations()
									.getTermsAggregation("groupby_user_age")
									.getBuckets()
									.stream()
									.collect(Collectors.toMap(TermsAggregation.Entry::getKey,TermsAggregation.Entry::getCount)));
	}

}
