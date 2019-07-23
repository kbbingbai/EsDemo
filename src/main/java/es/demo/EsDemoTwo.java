package es.demo;

import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.get.MultiGetRequest.Item;
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
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask.Status;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * 查询数据
 *
 * @Description:
 * @date 2019年3月22日
 */
public class EsDemoTwo {

    private TransportClient client = null;


    @Before
    public void init() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
    }


    // 得到一条数据
    @Test
    public void test1() {
        GetRequestBuilder prepareGet = client.prepareGet("per", "persion", "6");
        ListenableActionFuture<GetResponse> execute = prepareGet.execute();
        GetResponse actionGet = execute.actionGet();
        System.out.println(actionGet.getSourceAsMap());
        client.close();
    }

    // 数据的插入
    @Test
    public void test2() throws IOException {
        IndexRequestBuilder prepareIndex = client.prepareIndex("per", "persion", "21");
        IndexRequestBuilder setSource = prepareIndex.setSource(
                XContentFactory.jsonBuilder().startObject().field("name", "name21").field("age", "22").endObject());

        ListenableActionFuture<IndexResponse> execute = setSource.execute();
        IndexResponse actionGet = execute.actionGet();
        System.out.println(actionGet.getId());
        client.close();
    }

    // 删除一个文档
    @Test
    public void test3() {
        DeleteRequestBuilder prepareDelete = client.prepareDelete("per", "persion", "21");
        DeleteResponse deleteResponse = prepareDelete.get();
        Result result = deleteResponse.getResult();
        String string = result.toString();
        System.out.println(string);
        client.close();
    }

    // 更新一条数据，采用如下的两种方式
    @Test
    public void test4() throws IOException {
        UpdateRequestBuilder prepareUpdate = client.prepareUpdate("per", "persion", "9");
        UpdateRequestBuilder setDoc = prepareUpdate.setDoc(
                XContentFactory
                        .jsonBuilder()
                        .startObject()
                        .field("name", "name9")
                        .field("age", "9")
                        .endObject()
        );

        UpdateResponse updateResponse = setDoc.get();
        System.out.println(updateResponse.getIndex());
        System.out.println(updateResponse.getId());
        client.close();
    }

    @Test
    public void test5() throws IOException, InterruptedException, ExecutionException {

        UpdateRequest updateRequest = new UpdateRequest("per", "persion", "9");
        UpdateRequest doc = updateRequest.doc(
                XContentFactory
                        .jsonBuilder()
                        .startObject()
                        .field("name", "name9/dd//")
                        .field("age", "9")
                        .endObject()
        );
        UpdateResponse updateResponse = client.update(doc).get();
        System.out.println(updateResponse.getResult());
        client.close();
    }

    //批量查询的样子
    @Test
    public void test6() {
        MultiGetRequestBuilder prepareMultiGet = client.prepareMultiGet();
        Item item = new Item("per", "persion", "9");
        Item item2 = new Item("per", "persion", "5");
        MultiGetRequestBuilder add = prepareMultiGet.add(item).add(item2);
        MultiGetResponse multiGetResponse = add.get();
        Iterator<MultiGetItemResponse> iterator = multiGetResponse.iterator();
        while (iterator.hasNext()) {
            MultiGetItemResponse next = iterator.next();
            System.out.println(next.getResponse().getSourceAsString());
        }
    }

    //进行批量的操作bulk
    @Test
    public void test7() throws IOException {
        BulkRequestBuilder prepareBulk = client.prepareBulk();
        prepareBulk.add(
                new DeleteRequest("per", "persion", "2")
        );
        prepareBulk.add(
                new IndexRequest("per", "persion", "52")
                        .source(
                                XContentFactory
                                        .jsonBuilder()
                                        .startObject()
                                        .field("name", "name52")
                                        .field("age", 52)
                                        .endObject()
                        )
        );

        ListenableActionFuture<BulkResponse> execute = prepareBulk.execute();
        BulkResponse actionGet = execute.actionGet();
        Iterator<BulkItemResponse> iterator = actionGet.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next().getId());
        }
        client.close();
    }


    //删除掉符合条件的数据
    @Test
    public void test8() {
        BulkByScrollResponse bulkByScrollResponse = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("aa", "bb")).source("per").get();
        Status status = bulkByScrollResponse.getStatus();
        System.out.println(status.getDeleted());
        client.close();
    }


    //查询所有的数据
    @Test
    public void test33() {
        MatchAllQueryBuilder matchAllQuery = QueryBuilders.matchAllQuery();
        SearchRequestBuilder prepareSearch = client.prepareSearch("per");
        SearchResponse setQuery = prepareSearch.setQuery(matchAllQuery).get();
        SearchHits hits = setQuery.getHits();
        for (SearchHit temp : hits) {
            Map<String, Object> sourceAsMap = temp.getSourceAsMap();
            Set<String> keySet = sourceAsMap.keySet();
            for (String t : keySet) {
                System.out.println("key=" + t + "*****,value=" + sourceAsMap.get(t));
            }
        }
        client.close();
    }

    /**
     * match
     */
    @Test
    public void test313() {
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("name", "aa");
        SearchResponse response = client.prepareSearch("per").setQuery(matchQuery).setSize(3).get();
        SearchHits hits = response.getHits();
        for (SearchHit temp : hits) {
            Map<String, Object> sourceAsMap = temp.getSourceAsMap();
            Set<String> keySet = sourceAsMap.keySet();
            for (String t : keySet) {
                System.out.println(t + "===" + sourceAsMap.get(t));
            }
        }

        client.close();
    }


    @Test
    public void test31() {
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("name", "aa");
        SearchResponse response = client.prepareSearch("per").setQuery(matchQuery).setSize(3).get();
        SearchHits hits = response.getHits();
        for (SearchHit temp : hits) {
            Map<String, Object> sourceAsMap = temp.getSourceAsMap();
            Set<String> keySet = sourceAsMap.keySet();
            for (String t : keySet) {
                System.out.println(t + "===" + sourceAsMap.get(t));
            }
        }

        client.close();
    }

    //multiquery
    public void test32() {
        MultiMatchQueryBuilder query = QueryBuilders.multiMatchQuery("jingdong", "name", "age");
        SearchResponse response = client.prepareSearch("per").setQuery(query).setSize(3).get();
        SearchHits hits = response.getHits();
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
     */
    @Test
    public void test34() {
        TermQueryBuilder termQuery = QueryBuilders.termQuery("name", "jingdong");
        SearchResponse searchResponse = client.prepareSearch("pre").setQuery(termQuery).setSize(3).get();
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
     * @Description:
     */
    @Test
    public void test13() {
        TermsQueryBuilder termQuery = QueryBuilders.termsQuery("name", "jingdong", "name3");
        SearchResponse searchResponse = client.prepareSearch("per").setQuery(termQuery).setSize(4).get();
        SearchHits hits = searchResponse.getHits();
        for (SearchHit temp : hits) {
            System.out.println(temp.getSourceAsString());
        }
        client.close();
    }

    /***
     * range查询 ，查询
     */
    @Test
    public void test35() {
        RangeQueryBuilder query = QueryBuilders.rangeQuery("price").from("40").to("50");
        SearchResponse searchResponse = client.prepareSearch("ecommerce").setQuery(query).setSize(5).get();
        SearchHits hits = searchResponse.getHits();
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
        BoolQueryBuilder filter = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("name", "jingdong"))
                .mustNot(QueryBuilders.matchQuery("age", "40"))
                .should(QueryBuilders.matchQuery("age", "33"));

        SearchResponse searchResponse = client.prepareSearch("per").setQuery(filter).get();
        SearchHits hits = searchResponse.getHits();
        for (SearchHit temp : hits) {
            System.out.println(temp.getSourceAsString());
        }
        client.close();
    }

    /**
     * 按年龄进行聚合
     */
    @Test
    public void test45() {
        TermsAggregationBuilder agg = AggregationBuilders.terms("group_by_age").field("age");
        SearchResponse response = client.prepareSearch("per").addAggregation(agg).get();
        Terms maxPrice = response.getAggregations().get("group_by_age");
        for (Bucket entry : maxPrice.getBuckets()) {
            System.out.println(entry.getKey() + "*****" + entry.getDocCount());
        }
        client.close();
    }

    @Test
    public void test60() {
        TermsAggregationBuilder agg = AggregationBuilders.terms("group_by_age").field("age");
        ListenableActionFuture<SearchResponse> execute = client.prepareSearch("per").addAggregation(agg).execute();
        SearchResponse actionGet = execute.actionGet();
        Aggregations aggregations = actionGet.getAggregations();
        Terms maxPrice = aggregations.get("group_by_age");
        for (Bucket entry : maxPrice.getBuckets()) {
            System.out.println(entry.getKey() + "*****" + entry.getDocCount());
        }
        client.close();
    }


}
