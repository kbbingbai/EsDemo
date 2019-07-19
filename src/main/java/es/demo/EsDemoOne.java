package es.demo;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.sound.midi.Soundbank;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.TypeQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.tophits.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

/**
 * 查询数据
 * 
 * @Description:
 * @date 2019年3月22日
 */
public class EsDemoOne {

	private TransportClient client = null;

	// 集群的连接
	@Before
	public void test1() throws UnknownHostException {
		// 指定es集群名称
		Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
		// 指定集群的
		client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.2.205"), 9300));
	}

	/**
	 * 查询 数据 client.prepareGet()
	 */
	@Test
	public void getData() {
		GetRequestBuilder getRequest = client.prepareGet("es_hiveindex", "es_hivetype", "AWsqjbGRHwodk2VcZKOZ");
		ListenableActionFuture<GetResponse> execute = getRequest.execute();
		GetResponse response = execute.actionGet();
		System.out.println(response.getSourceAsString());
		client.close();
	}

	// 数据的插入

	/*
	 * PUT /index1 { "settings": { "number_of_replicas": 0, "number_of_shards":
	 * 3 }, "mappings": { "blog":{ "properties": { "id":{ "type": "long" },
	 * "title":{ "type": "text", "analyzer": "ik_max_word" }, "content":{
	 * "type": "text", "analyzer": "ik_max_word" }, "postdate":{ "type": "date"
	 * }, "url":{ "type": "text" } } } } }
	 */

	/**
	 * 1 创建一个document client.prepareIndex
	 * 注意：
	 *   1 IndexRequestBuilder.execute().actionGet(); 就相当于get
	 *   2 setSource 如果这个对象已经存在，不会对对象进行操作
	 * @Description:
	 */
	@Test
	public void test2() throws IOException {
		XContentBuilder doc = XContentFactory.jsonBuilder().startObject().field("id", "3").field("title", "设计模式这装饰模式")
				.field("content", "在不必改变动容的文件和使用继承的情况下，动态地扩展一个对象------")
				.field("url", "com.baidu").endObject();
		IndexRequestBuilder indexRequestBuilder = client.prepareIndex("index1", "blog", "9");
		IndexRequestBuilder setSource = indexRequestBuilder.setSource(doc);
		IndexResponse response = setSource.get();// 这个是真正的创建对象
		System.out.println(response.status());// 输出： CREATED
		doc.close();
		client.close();
	}
	

	/**
	 * 删除文档 client.prepareDelete
	 */

	@Test
	public void test3() {
		DeleteRequestBuilder prepareDelete = client.prepareDelete("index1", "blog", "10");
		DeleteResponse deleteResponse = prepareDelete.get();
		RestStatus status = deleteResponse.status();
		System.out.println(status);// 输出 OK
		client.close();
	}

	// 删除一个文档
	@Test
	public void test33() {
		DeleteRequestBuilder prepareDelete = client.prepareDelete("per", "persion", "21");
		DeleteResponse deleteResponse = prepareDelete.get();
		Result result = deleteResponse.getResult();
		String string = result.toString();
		System.out.println(string);// 打印DELETED
	}

	
	
	/**
	 * 修改文档,是update，不是覆盖 new UpdateRequest()
	 * 
	 * POST player_index/player_type/AWsriOCAHwodk2VcZKPX/_update
		{
		  "doc": {
		    "name":"t_name_update jvm"
		  }
		}
	 * 
	 */
	@Test
	public void test4() throws IOException, InterruptedException, ExecutionException {
		UpdateRequest updateRequest = new UpdateRequest();
		UpdateRequest request = updateRequest.index("index1").type("blog").id("10")
				.doc(XContentFactory.jsonBuilder().startObject().field("title", "java设计模式").endObject());

		UpdateResponse updateResponse = client.update(request).get();

		// System.out.println(updateResponse.getResult()); 它一般会输出两个值，一个值是：NOOP
		// 或者是 UPDATED
		// 如果我们修改的值与数据库的值一样，就是NOOP，说明我们的操作被 忽略了
		System.out.println(updateResponse.status());// 输出 OK
		client.close();
	}

	/**
	 * 修改文档,是update，不是覆盖,经过测试与上面的效果是一样的 client.prepareUpdate
	 */
	@Test
	public void test55() throws IOException {
		UpdateRequestBuilder prepareUpdate = client.prepareUpdate("per", "persion", "5");
		UpdateRequestBuilder setDoc = prepareUpdate
				.setDoc(XContentFactory.jsonBuilder().startObject().field("name", "name5").endObject());
		UpdateResponse updateResponse = setDoc.get();
		client.close();
	}

	/**
	 * upsert:如果文档存在，就修改文档(这个修改也是update)，如果文档不存在就添加文档 client.update
	 */
	@Test
	public void test5() throws IOException, InterruptedException, ExecutionException {
		IndexRequest indexRequest = new IndexRequest("index1", "blog", "8");
		IndexRequest request = indexRequest
				.source(XContentFactory.jsonBuilder().startObject().field("id", "2").field("title", "java设计修改1111")
						.field("content", "在不").field("postdate", "2018-04-06").field("url", "wwww").endObject());

		UpdateRequest updateRequest = new UpdateRequest("index1", "blog", "8");
		UpdateRequest upsert = updateRequest.doc(XContentFactory.jsonBuilder().startObject().field("title", "java设计添加")
				.field("content", "在不").field("postdate", "2018-04-06").field("url", "wwww").endObject())
				.upsert(request);

		ActionFuture<UpdateResponse> update = client.update(upsert);
		UpdateResponse updateResponse = update.get();
		System.out.println(updateResponse);
		client.close();
	}

	/**
	 * mget批量查询的方式 client.prepareMultiGet()
	 * 
	 GET player_index/_mget
		{
		  "docs": [
		    {
		      "_type":"player_type",
		      "_id":"AWsrh4PmHwodk2VcZKPT"
		    },
		    {
		      "_type":"player_type",
		      "_id":"AWsrhVV1Hwodk2VcZKPO"
		    }
		    ]
		}
	 */
	@Test
	public void test6() {
		MultiGetRequestBuilder prepareMultiGet = client.prepareMultiGet();
		/*
		 * 如果采用这样的方式，是不正确的，它只会查询出后台一条的数据 prepareMultiGet =
		 * prepareMultiGet.add("index1", "blog","1"); prepareMultiGet =
		 * prepareMultiGet.add("per", "persion","3","5");
		 */
		prepareMultiGet.add("index1", "blog", "8").add("per", "persion", "3", "5");
		MultiGetResponse multiGetResponse = prepareMultiGet.get();
		// Iterator<MultiGetItemResponse> iterator =
		// multiGetResponse.iterator();
		for (MultiGetItemResponse temp : multiGetResponse) {
			GetResponse response = temp.getResponse();
			if (response != null && response.isExists()) {
				System.out.println(response.getSourceAsString());
			}
		}
		client.close();
	}


	/**
	 * 进行批量的操作，bulk
	 * @throws IOException
	 */
	@Test
	public void getData7() throws IOException {
		BulkRequestBuilder prepareBulk = client.prepareBulk();

		prepareBulk.add(// 更新一个文件
				client.prepareUpdate("per", "persion", "3")
						.setDoc(XContentFactory.jsonBuilder().startObject().field("name", "name3").endObject()));

		prepareBulk.add(// 删除一个文档
				client.prepareDelete("per", "persion", "7"));
		BulkResponse bulkResponse = prepareBulk.get();// 注意要调用这个get才会真正的去执行
		client.close();
	}

	
	/**
	 * 把符合查询条件的数据删除掉,注意filter是or的关系
	 */
	@Test
	public void test8() {
		BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
				.filter(QueryBuilders.matchQuery("title", "java")).filter(QueryBuilders.matchQuery("content", "继承"))
				.source("index1")// 所在的索引
				.get();
		long deleted = response.getDeleted();
		System.out.println(deleted);
		client.close();
	}
	
	
	

	/**
	 * 查询所有match_all,查询前三个
	 */
	@Test
	public void test9() {
		MatchAllQueryBuilder matchAllQuery = QueryBuilders.matchAllQuery();
		SearchRequestBuilder prepareSearch = client.prepareSearch("player_index");
		SearchRequestBuilder setQuery = prepareSearch.setQuery(matchAllQuery);
		SearchResponse searchResponse = setQuery.get();
		SearchHits hits = searchResponse.getHits();
		for (SearchHit hit : hits) {
			Map<String, Object> sourceAsMap = hit.getSourceAsMap();
			Set<String> keySet = sourceAsMap.keySet();
			for (String key : keySet) {
				System.out.println("key=" + key + "*****,value=" + sourceAsMap.get(key));
			}
			System.out.println("---------------------------------------------");
		}
		client.close();
	}

	/**
	 * match
	 */
	@Test
	public void test10() {
		MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("name", "jingdong");
		SearchResponse searchResponse = client.prepareSearch("per").setQuery(matchQuery).setSize(3).get();
		SearchHits hits = searchResponse.getHits();
		for (SearchHit hit : hits) {
			Map<String, Object> sourceAsMap = hit.getSourceAsMap();
			Set<String> keySet = sourceAsMap.keySet();
			for (String key : keySet) {
				System.out.println("key=" + key + "*****,value=" + sourceAsMap.get(key));
			}
		}
		client.close();
	}

	/**
	 * multimatch multiMatchQuery("jingdong",
	 * "name","age");它的意思是字段name或者age中含有jingdong的数据
	 */
	@Test
	public void test11() {
		MultiMatchQueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery("jingdong", "name", "age");
		SearchResponse searchResponse = client.prepareSearch("per").setQuery(multiMatchQuery).setSize(3).get();
		SearchHits hits = searchResponse.getHits();
		for (SearchHit temp : hits) {
			Map<String, Object> sourceAsMap = temp.getSourceAsMap();
			for (String t : sourceAsMap.keySet()) {
				System.out.println("key=" + t + "*****,value=" + sourceAsMap.get(t));
			}
		}
		client.close();
	}

	/**
	 * term查询
	 * 
	 * @Description:
	 */
	@Test
	public void test12() {
		TermQueryBuilder termQuery = QueryBuilders.termQuery("name", "jingdong");
		SearchResponse searchResponse = client.prepareSearch("per").setQuery(termQuery).setSize(4).get();
		SearchHits hits = searchResponse.getHits();
		for (SearchHit temp : hits) {
			Map<String, Object> sourceAsMap = temp.getSourceAsMap();
			for (String t : sourceAsMap.keySet()) {
				System.out.println("key=" + t + "*****,value=" + sourceAsMap.get(t));
			}
		}
		client.close();
	}

	/**
	 * terms查询 ,查询name字段中包含jingdong或者name3的字段
	 * 
	 * @Description: 1:明确match 与 term 的区别 2:明确multimatch 与terms 的区别
	 */
	@Test
	public void test13() {
//		TermsQueryBuilder termQuery = QueryBuilders.termsQuery("name", "jingdong", "name3");
//		SearchResponse searchResponse = client.prepareSearch("myperson").setQuery(termQuery).setSize(4).get();
//		SearchHits hits = searchResponse.getHits();
//		for (SearchHit temp : hits) {
//			System.out.println(temp.getSourceAsString());
//		}
//		client.close();

		
	}

	/**
	 * range查询 ,查询价格在30-50(包括30到50)的商品
	 * 
	 * @Description:
	 */
	@Test
	public void test14() {
		RangeQueryBuilder termQuery = QueryBuilders.rangeQuery("price").from("40").to("55");
		// RangeQueryBuilder termQuery =
		// QueryBuilders.rangeQuery("birth").from("2018-03-03").to("2019-03-03").format("yyyy-MM-dd");
		SearchResponse searchResponse = client.prepareSearch("ecommerce").setQuery(termQuery).setSize(4).get();
		SearchHits hits = searchResponse.getHits();
		for (SearchHit temp : hits) {
			System.out.println(temp.getSourceAsString());
		}
		client.close();
	}
	
	
	
	

	/**
	 * prefix查询
	 * 
	 * @Description:
	 */
	@Test
	public void test15() {
		PrefixQueryBuilder prefixQuery = QueryBuilders.prefixQuery("name", "ya");
		// RangeQueryBuilder termQuery =
		// QueryBuilders.rangeQuery("birth").from("2018-03-03").to("2019-03-03").from("yyyy-MM-dd");
		SearchResponse searchResponse = client.prepareSearch("ecommerce").setQuery(prefixQuery).setSize(4).get();
		SearchHits hits = searchResponse.getHits();
		for (SearchHit temp : hits) {
			System.out.println(temp.getSourceAsString());
		}
		client.close();
	}

	/**
	 * ids查询
	 * 
	 * @Description:
	 */
	@Test
	public void test16() {
		IdsQueryBuilder idsQuery = QueryBuilders.idsQuery().addIds("1", "2", "3");
		SearchResponse searchResponse = client.prepareSearch("ecommerce").setTypes("product").setQuery(idsQuery)
				.setSize(4).get();
		SearchHits hits = searchResponse.getHits();
		for (SearchHit temp : hits) {
			System.out.println(temp.getSourceAsString());
		}
		client.close();
	}

	/**
	 * type查询
	 */
	@Test
	public void test17() {
		TypeQueryBuilder typeQuery = QueryBuilders.typeQuery("product");
		SearchResponse searchResponse = client.prepareSearch("ecommerce").setQuery(typeQuery).setSize(4).get();
		SearchHits hits = searchResponse.getHits();
		for (SearchHit temp : hits) {
			System.out.println(temp.getSourceAsString());
		}
		client.close();
	}

	/**
	 * queryString 1 commonTermsQuery 只能设置一个字段 2 queryStringQuery 是且的关系 3
	 * simpleQueryStringQuery 是或的关系
	 */
	@Test
	public void test19() {
		// CommonTermsQueryBuilder commonTermsQuery =
		// QueryBuilders.commonTermsQuery("name", "jingdong");
		// SearchResponse searchResponse =
		// client.prepareSearch("per").setQuery(commonTermsQuery).setSize(3).get();

		// +表示包含，-表示不包含，queryStringQuery是and的关系
		// QueryStringQueryBuilder queryStringQuery =
		// QueryBuilders.queryStringQuery("+jingdong -60");
		// SearchResponse searchResponse2 =
		// client.prepareSearch("per").setQuery(queryStringQuery).get();

		// +表示包含，-表示不包含，simpleQueryStringQuery是or的关系
		SimpleQueryStringBuilder simpleQueryStrin = QueryBuilders.simpleQueryStringQuery("+jingdong -60");
		SearchResponse searchResponse3 = client.prepareSearch("per").setQuery(simpleQueryStrin).get();

		SearchHits hits = searchResponse3.getHits();
		for (SearchHit temp : hits) {
			System.out.println(temp.getSourceAsString());
		}
		client.close();
	}

	/**
	 * 组合查询boolQuery
	 */
	@Test
	public void test20() {

		BoolQueryBuilder filter = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("name", "jingdong"))
				.mustNot(QueryBuilders.matchQuery("age", "40")).should(QueryBuilders.matchQuery("age", "33"))
				.filter(QueryBuilders.rangeQuery("age").gte("30"));

		SearchResponse searchResponse = client.prepareSearch("per").setQuery(filter).get();
		SearchHits hits = searchResponse.getHits();
		for (SearchHit temp : hits) {
			System.out.println(temp.getSourceAsString());
		}
		client.close();
	}
	
	

	/**
	 * aggregation terms 聚合，按年龄进行聚合
	 * 
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@Test
	public void test21() throws InterruptedException, ExecutionException {
		TermsAggregationBuilder agg = AggregationBuilders.terms("group_by_age").field("age");
		SearchResponse response = client.prepareSearch("per").addAggregation(agg).execute().actionGet();
		Terms maxPrice = response.getAggregations().get("group_by_age");
		for (Terms.Bucket entry : maxPrice.getBuckets()) {
			System.out.println(entry.getKey() + "*****" + entry.getDocCount());
		}
		client.close();
	}

	/**
	 * aggregation求最大值，最小值，平均值等
	 */
	@Test
	public void test18() {
		MaxAggregationBuilder agg = AggregationBuilders.max("maxValue").field("price");
		SearchResponse response = client.prepareSearch("ecommerce").addAggregation(agg).execute().actionGet();
		Max maxPrice = response.getAggregations().get("maxValue");
		System.out.println(maxPrice.getValue());
		client.close();
	}

	/**
	 * 一个过滤条件的统计 filter AggregationBuilders.filter(String name, QueryBuilder
	 * filter)
	 */
	@Test
	public void test22() {
		TermQueryBuilder ageFilter = QueryBuilders.termQuery("age", "40");
		FilterAggregationBuilder agg = AggregationBuilders.filter("ageFilter", ageFilter);
		SearchResponse response = client.prepareSearch("per").addAggregation(agg).execute().actionGet();
		InternalFilter aggregation = response.getAggregations().get("ageFilter");
		System.out.println(aggregation.getDocCount());
		client.close();
	}

	/**
	 * 多个过滤条件的统计 filters
	 */
	@Test
	public void test23() {
		FiltersAggregationBuilder agg = AggregationBuilders.filters("filters",
				new KeyedFilter("ageIs33", QueryBuilders.termQuery("age", "33")),
				new KeyedFilter("ageIs50", QueryBuilders.termQuery("age", "50")));

		SearchResponse response = client.prepareSearch("per").addAggregation(agg).execute().actionGet();
		Filters aggregation = response.getAggregations().get("filters");
		for (Filters.Bucket entry : aggregation.getBuckets()) {
			System.out.println(entry.getKey() + "*****" + entry.getDocCount());
		}
		client.close();
	}

	/**
	 * 范围统计
	 */
	@Test
	public void test24() {
		RangeAggregationBuilder agg = AggregationBuilders.range("ageRange").field("age").addRange(35, 40)
				.addUnboundedFrom(50).addUnboundedTo(45);

		SearchResponse response = client.prepareSearch("per").addAggregation(agg).execute().actionGet();

		Range aggregation = response.getAggregations().get("ageRange");
		for (Range.Bucket entry : aggregation.getBuckets()) {
			System.out.println(entry.getKey() + "*****" + entry.getDocCount());
		}

		client.close();
	}

	/**
	 * missing 统计字段的值是null数据
	 */
	@Test
	public void test25() {
		MissingAggregationBuilder agg = AggregationBuilders.missing("missing").field("age");
		SearchResponse response = client.prepareSearch("per").addAggregation(agg).execute().actionGet();

		Missing aggregation = response.getAggregations().get("missing");
		System.out.println(aggregation.getDocCount());
		client.close();
	}

	
	/**
	 * 统计每个作者的文章的数量，并取出每个作者的前三个作品
	 * @Description:
	 */
	@Test
	public void test26() {
		TopHitsAggregationBuilder subAgg = AggregationBuilders.topHits("top_blogs").size(3).fetchSource(new String[]{"content","title"}, new String[]{});
		TermsAggregationBuilder userAgg = AggregationBuilders.terms("group_by_userName")
				.field("userInfo.userName.keyword").subAggregation(subAgg);
		SearchResponse searchResponse = client.prepareSearch("website").addAggregation(userAgg).get();
		Terms userNameTerm = searchResponse.getAggregations().get("group_by_userName");
		
		System.out.println(userNameTerm.getType()+"***********");
		
		
		for (Terms.Bucket entry : userNameTerm.getBuckets()) {
			System.out.println("数量 ："+entry.getDocCount()+","+entry.getKey());
			InternalTopHits blogsTerm = entry.getAggregations().get("top_blogs");
			SearchHits hits = blogsTerm.getHits();
			for (SearchHit hit : hits) {
				Map<String, Object> sourceAsMap = hit.getSourceAsMap();
				Set<String> keySet = sourceAsMap.keySet();
				for (String key : keySet) {
					System.out.println("key=" + key + "*****,value=" + sourceAsMap.get(key));
				}
			}
		}
		client.close();
	}
	
	/**
	 * 计算每个部门的男性员工数
	 * @Description:
	 * select deptid, count(*) as emp_count from employee group by deptid where sex=0
	 */
	@Test
	public void test27(){
		TermsAggregationBuilder agg = AggregationBuilders.terms("group_by_dep").field("userName");
		SearchRequestBuilder addAggregation = client.prepareSearch("per")
													.setTypes("persion")
													.setQuery(QueryBuilders.termQuery("sex", 1))
													.addAggregation(agg);
		
		SearchResponse searchResponse = addAggregation.get();
		Terms aggregation = searchResponse.getAggregations().get("group_by_dep");
		for (Terms.Bucket entry : aggregation.getBuckets()) {
			System.out.println(entry.getKey() + "*****" + entry.getDocCount());
		}
	}
	
	
	/***
	 * 2：group by多个field，与test26差不多
	 * 例如要计算每个球队每个位置的球员数，如果使用SQL语句，应表达如下：
		select deptid, birthplace, count(*) as emp_count from employee group by deptid, birthplace
		输出的结果：
	
			部门编号=8
			籍贯=广东佛山;员工数=9
			籍贯=湖北武汉;员工数=8
			籍贯=湖南长沙;员工数=5
			籍贯=广东深圳;员工数=4
			籍贯=广东广州;员工数=3
			籍贯=湖南岳阳;员工数=2
			部门编号=5
			籍贯=湖北武汉;员工数=7
			籍贯=广东佛山;员工数=4
			籍贯=广东广州;员工数=4
			籍贯=广东深圳;员工数=4
			籍贯=湖南岳阳;员工数=3
			籍贯=湖南长沙;员工数=3
			部门编号=6
			籍贯=广东广州;员工数=6
			籍贯=广东佛山;员工数=4
			籍贯=广东深圳;员工数=4
			籍贯=湖南岳阳;员工数=4
			籍贯=湖北武汉;员工数=2
			籍贯=湖南长沙;员工数=2
			部门编号=7
			籍贯=广东广州;员工数=6
			籍贯=湖南岳阳;员工数=6
			籍贯=广东深圳;员工数=4
			籍贯=湖北武汉;员工数=3
			籍贯=广东佛山;员工数=2
			籍贯=湖南长沙;员工数=1
	 */
	 public static void groupByMutilFieldTest(TransportClient client) {
	        SearchRequestBuilder requestBuilder = client.prepareSearch("per").setTypes("persion");
	        TermsAggregationBuilder aggregationBuilder1 = AggregationBuilders.terms("emp_count").field("deptid");
	        TermsAggregationBuilder aAggregationBuilder2 = AggregationBuilders.terms("region_count").field("birthplace");
	        requestBuilder.addAggregation(aggregationBuilder1.subAggregation(aAggregationBuilder2));
	        SearchResponse response = requestBuilder.execute().actionGet();
	        Terms terms1 = response.getAggregations().get("emp_count");

	        Terms terms2;
	        for (Terms.Bucket bucket : terms1.getBuckets()) {
	            System.out.println("部门编号=" + bucket.getKey());
	            terms2 = bucket.getAggregations().get("region_count");
	            for (Terms.Bucket bucket2 : terms2.getBuckets()) {
	                System.out.println("籍贯=" + bucket2.getKey() + ";员工数=" + bucket2.getDocCount());
	            }
	        }
	    }
	
	
	/**
	 * max/min/sum/avg
	 *  select deptid, max(salary) as max_salary from employee group by deptid
	 *  例如 计算每个部门最高的工资，如果使用SQL语句，应表达如下：
	 *  复制代码
		 输出结果：
		 部门编号=8;最高工资=8000.0
		 部门编号=5;最高工资=8000.0
		 部门编号=6;最高工资=8000.0
		 部门编号=7;最高工资=8000.0
	 */
	 public static void maxTest(TransportClient client) {
	 	 SearchRequestBuilder requestBuilder = client.prepareSearch("per").setTypes("persion");
         TermsAggregationBuilder aggregationBuilder1 = AggregationBuilders.terms("deptid").field("deptid");
         MaxAggregationBuilder aggregationBuilder2 = AggregationBuilders.max("maxsalary").field("salary");
         requestBuilder.addAggregation(aggregationBuilder1.subAggregation(aggregationBuilder2));
         SearchResponse response = requestBuilder.execute().actionGet();

         Terms aggregation = response.getAggregations().get("deptid");
         Max terms2 = null;
         for (Terms.Bucket bucket : aggregation.getBuckets()) {
             terms2 = bucket.getAggregations().get("maxsalary");  //class org.elasticsearch.search.aggregations.metrics.max.InternalMax
             System.out.println("部门编号=" + bucket.getKey() + ";最高工资=" + terms2.getValue());
         }
     }

	 /**
	  * 4：对多个field求max/min/sum/avg
		例如要计算每个部门的平均年龄，同时又要计算总薪资，最后按平均年龄升序排序，如果使用SQL语句，应表达如下：
		select deptid, avg(age) as avg_age, sum(salary) as max_salary from employee group by deptid order by avg_age asc
	  */
	 public static void methodTest1(TransportClient client) {
		SearchRequestBuilder requestBuilder = client.prepareSearch("per").setTypes("persion");
        TermsAggregationBuilder aggregationBuilder1 = AggregationBuilders.terms("deptid").field("deptid").order((List<Terms.Order>) Order.aggregation("avg_age", true)); //按平均年龄升序排序,
        AggregationBuilder aggregationBuilder2 = AggregationBuilders.avg("avg_age").field("age");
        AggregationBuilder aggregationBuilder3 = AggregationBuilders.sum("sum_salary").field("salary");
        requestBuilder.addAggregation(aggregationBuilder1.subAggregation(aggregationBuilder2).subAggregation(aggregationBuilder3));
        SearchResponse response = requestBuilder.execute().actionGet();
        Terms aggregation = response.getAggregations().get("deptid");
        Avg terms2 = null;
        Sum term3 = null;
        for (Terms.Bucket bucket : aggregation.getBuckets()) {
            terms2 = bucket.getAggregations().get("avg_age"); // org.elasticsearch.search.aggregations.metrics.avg.InternalAvg
            term3 = bucket.getAggregations().get("sum_salary"); // org.elasticsearch.search.aggregations.metrics.sum.InternalSum
            System.out.println("部门编号=" + bucket.getKey() + ";平均年龄=" + terms2.getValue() + ";总工资=" + term3.getValue());
        }
    }
	 
	
	 /**
	  * 创建mapping信息并创建索引 
	 * @throws IOException 
	  */
	 @Test
	 public void test40() throws IOException{
		 XContentBuilder mappings = XContentFactory.jsonBuilder()
			.startObject()
				.startObject("settings")
				.field("number_of_shards",3)
				.field("number_of_replicas", 1)
				.endObject()
			.endObject()
			.startObject()
				.startObject("pingguo")
					.startObject("properties")
						.startObject("type").field("type","string").field("store", "yes")
						.endObject()
						.startObject("eventCount").field("type", "long").field("store","yes")
						.endObject()
						.startObject("eventDate").field("type", "date").field("store", "yes")
						.field("format", "dateOptionalTime").field("store","yes")
						.endObject()
						.startObject("message").field("type", "string")
							.field("index","not_analyzed").field("store","yes")
						.endObject()
					.endObject()
				.endObject()
			.endObject();
						
				
		CreateIndexRequestBuilder setSource = client.admin().indices().prepareCreate("fruit").setSource(mappings);
		
		CreateIndexResponse createIndexResponse = setSource.get();
		
		if(createIndexResponse.isAcknowledged()){
			System.out.println("index created success");
		}else{
			System.out.println("index create faild");
		}
	 }
 
	 
	 @Test
	 public void test(){
		 
		 BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("name", "name")).must(QueryBuilders.termQuery("desc", "texqqt"));
		 SearchResponse searchResponse = client.prepareSearch("myperson").setTypes("per").setQuery(query).get();
		 System.out.println(searchResponse.getHits().iterator().next().getId());
		
		 
		client.close();
	 }
	 
	 
	 
	 
	
	
	
	
	

}
