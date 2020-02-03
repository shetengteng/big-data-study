# 电商的搜索列表功能



## 功能简介

- 入口
  - 首页分类
  ![](../img/es (1).png) 
  - 搜索栏
  
   ![](../img/es (2).png)

- 列表展示

![](../img/es (3).png) 



## 依据业务搭建数据结构

这时我们要思考三个问题：

1、 哪些字段需要分词

2、 我们用哪些字段进行过滤

3、 哪些字段我们需要通过搜索显示出来

| 分析                       |                                      |                  |
| -------------------------- | ------------------------------------ | ---------------- |
| 需要分词的字段             | 名字，描述                           | 分词、定义分词器 |
| 有可能用于过滤或统计的字段 | 平台属性值、三级分类、价格、排序评分 | 要索引，不分词   |
| 其他需要显示的字段         | 图片路径、skuId                      | 不索引           |

- 结构定义

```bash
PUT gmall
{
  "mappings": {
    "SkuInfo":{
      "properties": {
        "id":{
          "type": "keyword"
          , "index": false
        },
        "price":{
          "type": "double"
        },
         "skuName":{
          "type": "text",
          "analyzer": "ik_max_word"
        },
        "skuDesc":{
          "type": "text",
          "analyzer": "ik_smart"
        },
        "catalog3Id":{
          "type": "keyword"
        },
        "skuDefaultImg":{
          "type": "keyword",
          "index": false
        },
        "hotScore":{
          "type": "long"
        },
        "skuAttrValueList":{
          "properties": {
            "valueId":{
              "type":"keyword"
            }
          }
        }
      }
    }
  }
}
```



## SKU 数据保存到ES

- 将mysql中的数据导入到ES
- ES中的mapping定义完成后，不可修改，如果要修改，需要重新建立mapping，重新导入数据

- es存储数据是以json格式保存的，那么如果一个javabean的结构刚好跟要求的json格式吻合就可以直接把javaBean序列化为json保持到es中，所以我们要制作一个与es中json格式一致的javabean



### bean

```java
public class SkuLsInfo implements Serializable {
	String id;
	BigDecimal price;
	String skuName;
    String skuDesc;
    String catalog3Id;
    String skuDefaultImg;
    Long hotScore;
    List<SkuLsAttrValue> skuAttrValueList;
}
```

```java
public class SkuLsAttrValue implements Serializable {
    String valueId;
}
```



### 实现类

- 在实现类中添加方法

```java
public void saveSkuInfoLs(SkuLsInfo skuLsInfo){
    Index index= new Index
        .Builder(skuLsInfo) // 会将bean转换成json存储到ES中
        .index("gmall") // 库名
        .type("SKUInfo") // 表名
        .id(skuLsInfo.getId()) // 可以指定id，如果指定则id属性和ES的_id 值相同
        .build();
    try {
        jestClient.execute(index);
    } catch (IOException e) {
        e.printStackTrace();
    }
}
```

- 从数据库获取解析成bean，将该信息保存到ES中

```java
public int loadAllItem(){
    List<SkuInfo> skuInfoList = manageService.getSkuInfoList();

    for (SkuInfo skuInfo : skuInfoList) {
        SkuLsInfo skuLsInfo = new SkuLsInfo();
        try {
            BeanUtils.copyProperties(skuLsInfo,skuInfo);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

        List<SkuLsAttrValue> skuAttrValueEsList =new ArrayList<>();

        List<SkuAttrValue> skuAttrValueList = skuInfo.getSkuAttrValueList();
        for (SkuAttrValue skuAttrValue : skuAttrValueList) {
            SkuLsAttrValue skuAttrValueEs = new SkuLsAttrValue();
            skuAttrValueEs.setValueId(skuAttrValue.getValueId());
            skuAttrValueEsList.add(skuAttrValueEs);
        }

        skuLsInfo.setSkuAttrValueList(skuAttrValueEsList);
		// 保存到ES中
        saveSkuInfoLs(skuLsInfo);//异步
    }
    return skuInfoList.size();
}
```



## 查询数据



### 分析

先观察功能页面，一共要用什么查询条件，查询出什么数据？

查询条件： 

1、 关键字

2、 可通过**分类**进入列表页面

3、 属性值

4、 分页页码

查询结果： 

1  sku的列表(关键字高亮显示)

2 这些sku涉及了哪些属性和属性值

3 命中个数，用于分页



### 编写DSL

- 查询小米手机 4G

```bash
GET /gmall/SKUInfo/_search
{
	"query":{
		"bool":{
			"filter":[
				{"term":{"skuAttrValueList.valueId": "8"}},# 8表示4G属性
            	{"term":{"skuAttrValueList.valueId": "4"}},
            	{"term":{"catalog3Id":"61"}} # 3级分类
            ],
			"must":{
				"match":{"skuName":"小米"}
			}
		}
	},
	"highlight": { # 高亮处理
		"fields": {"skuName":{}},
		"pre_tags": "<span style='color:red'>",
		"post_tags": "</span>"
	},
	"from": 0, # 分页
	"size": 20,
	"sort": [{"hotScore":{"order":"desc"}}], # 排序
	"aggs":{ # 聚合，显示相同属性值
		"groupby_valueId":{
			"terms": {
				"field":"skuAttrValueList.valueId"
			}
		}
	}
}
```



### 传入参数bean

```java
public class SkuLsParams implements Serializable {
    String keyword;  //关键词
    String catalog3Id;  //三级分类
    String[] valueId;   //属性列表 用于过滤
    int pageNo=1; //页码，默认第1页
    int pageSize=20;//每页显示商品个数
}
```



### 返回结果bean

```java
public class SkuLsResult implements Serializable {
    List<SkuLsInfo> skuLsInfoList;
    long total;
    long totalPages;
    List<String> attrValueIdList;
}
```



### 构造查询DSL

- jest客户端包，提供了一组builder工具
  - 可比较方便的帮程序员组合复杂的查询Json

```java
private String makeQueryStringForSearch(SkuLsParams skuLsParams){
    
    SearchSourceBuilder searchSourceBuilder=new SearchSourceBuilder();
    BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
    
    searchSourceBuilder.query(boolQueryBuilder);
    
    //按属性值过滤 filter
    if(skuLsParams.getValueId()!=null&&skuLsParams.getValueId().length>=0){
        List<String> list = Arrays.asList(skuLsParams.getValueId());
        for (int i = 0; i < skuLsParams.getValueId().length; i++) {
            String valueId = skuLsParams.getValueId()[i];
            boolQueryBuilder.filter(
                new TermsQueryBuilder("skuAttrValueList.valueId",valueId));
        }
    }
    
    //按三级分类查询 filter
    if(skuLsParams.getCatalog3Id()!=null){
        QueryBuilder termQueryBuilder=new TermQueryBuilder("catalog3Id",skuLsParams.getCatalog3Id());
        boolQueryBuilder.filter(termQueryBuilder);
    }
    
    //录入关键词 must match
    if(skuLsParams.getKeyword()!=null){
        boolQueryBuilder.must(
            new MatchQueryBuilder("skuName",skuLsParams.getKeyword()));

        HighlightBuilder highlightBuilder=new HighlightBuilder();
        highlightBuilder.field("skuName");
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        searchSourceBuilder.highlight(highlightBuilder);

    }
    //利用页号和每页个数 计算其实位置
    searchSourceBuilder.from((skuLsParams.getPageNo()-1)*skuLsParams.getPageSize());
    searchSourceBuilder.size(skuLsParams.getPageSize());
    
    //按评分倒序
    searchSourceBuilder.sort("hotScore",SortOrder.DESC);
    
    //聚合  查询出所有涉及的属性值
    TermsBuilder groupby_attr = AggregationBuilders.terms("groupby_attr").field("skuAttrValueList.valueId");
     searchSourceBuilder.aggregation(groupby_attr);

    String query = searchSourceBuilder.toString();
    System.err.println("query = " + query);
    return query;
}
```



### 返回值处理

- 所有的返回值其实都在这个searchResult中

```java
searchResult = jestClient.execute(search);
```

- 结果结构

![](../img/es (4).png) 

![](../img/es (5).png)  

![](../img/es (6).png) 

```java
private SkuLsResult makeResultForSearch(SkuLsParams skuLsParams,SearchResult searchResult){

    //获取sku列表
    List<SearchResult.Hit<SkuLsInfo, Void>> hits =
        searchResult.getHits(SkuLsInfo.class);
    
    List<SkuLsInfo> skuLsInfoList=new ArrayList<>(skuLsParams.getPageSize());
    for (SearchResult.Hit<SkuLsInfo, Void> hit : hits) {
         // source构建成对象
        SkuLsInfo skuLsInfo = hit.source; 
        // 获取高亮
        if(hit.highlight!=null&&hit.highlight.size()>0){
            //把带有高亮标签的字符串替换skuName
            skuLsInfo.setSkuName(hit.highlight.get("skuName").get(0));
        }
        skuLsInfoList.add(skuLsInfo);
    }
    SkuLsResult returnResult=new SkuLsResult();
    returnResult.setSkuLsInfoList(skuLsInfoList);
    returnResult.setTotal(searchResult.getTotal());

    //取记录个数并计算出总页数
    long totalPage= (searchResult.getTotal() + skuLsParams.getPageSize() -1) / skuLsParams.getPageSize();
    returnResult.setTotalPages(totalPage);

    //取出涉及的属性值id
    MetricAggregation aggregations = searchResult.getAggregations();
    TermsAggregation groupby_attr = aggregations.getTermsAggregation("groupby_attr");
    if(groupby_attr!=null){
        List<String> attrValueIdList=new ArrayList<>();
        for (TermsAggregation.Entry bucket : groupby_attr.getBuckets()) {
            attrValueIdList.add( bucket.getKey()) ;
        }
        returnResult.setAttrValueIdList(attrValueIdList);
    }
    return returnResult;
}
```



## 排序 [更新hotScore]

- 页面结构完成了，考虑一下如何排序，es查询的dsl语句中我们是用了hotScore来进行排序的。

- 但是hotScore从何而来，根据业务去定义，也可以扩展更多类型的评分，让用户去选择如何排序

- 这里的hotScore我们假定以点击量来决定热度
  - 那么每次用户点击，将这个评分+1

### 问题

- es大量的写操作会影响es 性能
  - es需要更新索引
  - es不是内存数据库，会落盘
  - ==会做相应的io操作==

- 修改某一个值，在高并发情况下会有冲突，造成更新丢失
  - 需要加锁
  - es的乐观锁会恶化性能问题
- 从业务角度出发，其实为商品进行排序所需要的热度评分，并不需要非常精确
  - 利用这个特点可稀释掉大量写操作



### 思路

- 用redis做精确计数器
  - redis是内存数据库读写性能都非常快
  - 利用redis的原子性的自增可以解决并发写操作

- redis每计100次数（可以被100整除）我们就更新一次es 
  - 写操作就被稀释了100倍
  - 倍数可以根据业务情况灵活设定



### 代码

- 更新ES

 ```java
private void updateHotScore(String skuId,Long hotScore){
    String updateJson="{\n" +
        "   \"doc\":{\n" +
        "     \"hotScore\":"+hotScore+"\n" +
        "   }\n" +
        "}";
    Update update = new Update
        .Builder(updateJson)
        .index("gmall")
        .type("SkuInfo")
        .id(skuId)
        .build();
    try {
        jestClient.execute(update);
    } catch (IOException e) {
        e.printStackTrace();
    }
}
 ```

- 更新redis
  - 每次点击数满100，更新一次ES

```java
//更新热度评分
@Override
public void incrHotScore(String skuId){
    Jedis jedis = redisUtil.getJedis();
    int timesToEs=100;
    // 使用zset进行存储
    Double hotScore = jedis.zincrby("hotScore", 1, "skuId:" + skuId);
    if(hotScore%timesToEs==0){
        updateHotScore(skuId, Math.round(hotScore));
    }
}
```

- 问题，redis宕机数据如何恢复（RDB，AOF）

