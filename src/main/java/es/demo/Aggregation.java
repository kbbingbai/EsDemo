package es.demo;

import junit.framework.TestResult;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * date  2019/7/27-15:29
 * Description: 测试es有聚合操作
 * 运行的结果：
 */
public class Aggregation {

    private TransportClient client = null;

    // 集群的连接
    @Before
    public void test1() throws UnknownHostException {
        // 指定es集群名称
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
        // 指定集群的
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
    }

    /**
     * 2019-07-27
     * 文档中有一个字段是age,我们可以用这个求这个字段的 sum,avg,min,max,该方法只演示了求平均值，
     * 其它的几个就不演示了，道理都是一样的
     * @Description:
     */
    @Test
    public void test02() throws IOException {
        AvgAggregationBuilder ageOfAvg = AggregationBuilders.avg("age_of_avg").field("age");
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("2019-06").addAggregation(ageOfAvg);
        SearchResponse searchResponse = searchRequestBuilder.get();
        Aggregations aggregations = searchResponse.getAggregations();
        Avg age_of_avg = aggregations.get("age_of_avg");
        double value = age_of_avg.getValue();
        System.out.println(value);
        client.close();
    }

    /**
     * 2019-07-27
     * 文档中有一个字段是age,我们看这个字段当中有几个不同的值
     * @Description:
     */
    @Test
    public void test03() throws IOException {
        CardinalityAggregationBuilder cardinality = AggregationBuilders.cardinality("age_of_cardinality").field("age");
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("2019-06").addAggregation(cardinality);
        SearchResponse searchResponse = searchRequestBuilder.get();
        Aggregations aggregations = searchResponse.getAggregations();
        Cardinality age_of_cardinality = aggregations.get("age_of_cardinality");
        double value = age_of_cardinality.getValue();
        System.out.println(value);
        client.close();
    }

    /**
     *
     * 一个复杂的聚合查询
     * 2019-07-27
     GET 2019-08/_search
     {
         "size": 0,
         "query": {
             "match": {
                "inters": "ks"
             }
         },
         "aggs": {
             "age_of_group": {
                 "terms": {
                     "field": "age",
                     "order": {
                        "age_of_avg": "desc"
                     }
                 },
                 "aggs": {
                     "age_of_avg": {
                         "avg": {
                            "field": "age"
                         }
                     }
                 }
             }
         }
     }
     查询的结果是：
     {
     "took": 1,
     "timed_out": false,
     "_shards": {
         "total": 5,
         "successful": 5,
         "skipped": 0,
         "failed": 0
     },
     "hits": {
         "total": 8,
         "max_score": 0,
         "hits": []
     },
     "aggregations": {
         "age_of_group": {
             "doc_count_error_upper_bound": 0,
             "sum_other_doc_count": 0,
             "buckets": [
                 {
                     "key": 78,
                     "doc_count": 1,
                     "age_of_avg": {
                        "value": 78
                     }
                 },
                 {
                     "key": 28,
                     "doc_count": 4,
                     "age_of_avg": {
                        "value": 28
                     }
                 },
                 {
                     "key": 20,
                     "doc_count": 2,
                     "age_of_avg": {
                        "value": 20
                     }
                 },
                 {
                     "key": 10,
                     "doc_count": 1,
                     "age_of_avg": {
                        "value": 10
                     }
                 }
             ]
         }
     }
     }

     * 实现复杂的聚合查询
     * @Description:
     */
    @Test
    public void test04() throws IOException {
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("inters", "ks");
        Terms.Order age_of_avg = Terms.Order.aggregation("age_of_avg", true);
        TermsAggregationBuilder order = AggregationBuilders.terms("age_of_group").field("age").order(age_of_avg);
        AvgAggregationBuilder field = AggregationBuilders.avg("age_of_avg").field("age");
        TermsAggregationBuilder termsAggregationBuilder = order.subAggregation(field);
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("2019-08").addAggregation(termsAggregationBuilder);
        SearchResponse searchResponse = searchRequestBuilder.get();
        Aggregations aggregations = searchResponse.getAggregations();
        Terms age_of_group = aggregations.get("age_of_group");
        final List<? extends Terms.Bucket> buckets = age_of_group.getBuckets();
        for (Terms.Bucket temp:buckets) {
            long key = (Long)temp.getKey();
            long docCount = temp.getDocCount();

            System.out.println("key="+key+"  count="+docCount);
            Aggregations aggregations1 = temp.getAggregations();
            Avg age_of_avg1 = aggregations1.get("age_of_avg");
            double value = age_of_avg1.getValue();

            System.out.println("子聚合的值="+value);
        }
        client.close();
    }

}
