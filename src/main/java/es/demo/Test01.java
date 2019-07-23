package es.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;


public class Test01 {
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
     * 创建mapping信息并创建索引
     *
     * @throws IOException
     */
    @Test
    public void test01() throws IOException {
        XContentBuilder mappings = XContentFactory.jsonBuilder().startObject().startObject("settings")
                .field("number_of_shards", 3).field("number_of_replicas", 1).endObject().endObject().startObject()
                .startObject("pingguo").startObject("properties").startObject("type").field("type", "string")
                .field("store", "yes").endObject().startObject("eventCount").field("type", "long").field("store", "yes")
                .endObject().startObject("eventDate").field("type", "date").field("store", "yes")
                .field("format", "dateOptionalTime").field("store", "yes").endObject().startObject("message")
                .field("type", "string").field("index", "not_analyzed").field("store", "yes").endObject().endObject()
                .endObject().endObject();

        CreateIndexRequestBuilder setSource = client.admin().indices().prepareCreate("fruit").setSource(mappings);

        CreateIndexResponse createIndexResponse = setSource.get();

        if (createIndexResponse.isAcknowledged()) {
            System.out.println("index created success");
        } else {
            System.out.println("index create faild");
        }

        client.close();

    }

    /**
     * 创建一个文档
     *
     * @Description:
     */
    @Test
    public void test02() throws IOException {
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().field("type", "syslog").field("eventCount", 1)
                .field("eventDate", new Date()).field("message", "secilog insert doc test").endObject();
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex("fruit", "pingguo", "1").setSource(doc);
        IndexResponse indexResponse = indexRequestBuilder.get();
        System.out.println(indexResponse.getId());
        client.close();
    }

    /**
     * 创建一个文档
     *
     * @Description:
     */

    @Test
    public void test03() throws IOException, InterruptedException, ExecutionException {
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().field("type", "syslog").field("eventCount", 2)
                .field("eventDate", new Date()).field("message", "secilog insert doc test").endObject();
        IndexRequest indexRequest = new IndexRequest("fruit", "pingguo").source(doc);
        IndexResponse indexResponse = client.index(indexRequest).get();
        System.out.println(indexResponse.getId());
        client.close();
    }

    /**
     * 修改一个文档
     *
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void test04() throws IOException, InterruptedException, ExecutionException {
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().field("type", "syslogUpdate").endObject();
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("fruit").type("pingguo").id("1").doc(doc);
        UpdateResponse updateResponse = client.update(updateRequest).get();
        System.out.println(updateResponse.getId());
        client.close();
    }

    /**
     * 查询文档GetRequest
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void test05() throws InterruptedException, ExecutionException {
        GetRequest getRequest = new GetRequest("fruit", "pingguo", "1");
        GetResponse getResponse = client.get(getRequest).get();

        System.out.println(getRequest);
        client.close();
    }

    /**
     * get
     */
    @Test
    public void test06() throws InterruptedException, ExecutionException {
        ArrayList<String> list = new ArrayList();
        list.add("AWu2fRqxOQQAeNRICqm6");
        list.add("AWu2WVOzpPFtF-ABiKKi");
        MultiGetRequestBuilder add = client.prepareMultiGet().add("teacher", "teacher", list);
        MultiGetResponse response = add.get();
        for (MultiGetItemResponse temp : response) {
            System.out.println(temp.getResponse().getSourceAsString());
        }
        client.close();
    }

    /**
     * 批量导出成json文件
     */

    @Test
    public void test07() {
        try {

            SearchResponse response = client.prepareSearch("dnsindex").setTypes("dnstype").setSize(500)
                    .setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
            SearchHits resultHits = response.getHits();

            File article = new File("./bulk.txt");
            FileWriter fw = new FileWriter(article);
            BufferedWriter bfw = new BufferedWriter(fw);

            if (resultHits.getHits().length == 0) {
                System.out.println("查到0条数据!");

            } else {
                for (int i = 0; i < resultHits.getHits().length; i++) {
                    String jsonStr = JSON.toJSONString(resultHits.getHits()[i].getSourceAsMap());
                    System.out.println(jsonStr);
                    bfw.write(jsonStr);
                    bfw.write("\n");
                }
            }
            bfw.close();
            fw.close();

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        client.close();
    }

    /**
     * 批量导入json数据
     *
     * @Description:
     */
    @Test
    public void test08() {
        try {
            File article = new File("./bulk.txt");
            FileReader fr = new FileReader(article);
            BufferedReader bfr = new BufferedReader(fr);
            String line = null;
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            int count = 0;
            while ((line = bfr.readLine()) != null) {
                System.out.println(count + "--------------");
                bulkRequest.add(client.prepareIndex("new_dnsindex", "new_dnstype").setSource(line, XContentType.JSON));
                if (count % 10 == 0) {
                    bulkRequest.execute().actionGet();
                    bulkRequest = client.prepareBulk();
                }
                count++;

            }
            bulkRequest.execute().actionGet();

            bfr.close();
            fr.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        client.close();
    }

    /**
     * 测试日期字段
     *
     * @throws IOException
     */
    @Test
    public void test09() throws IOException {
        IndexRequestBuilder prepareIndex = client.prepareIndex("2019-07-04", "type");
        IndexRequestBuilder setSource = prepareIndex
                .setSource(XContentFactory.jsonBuilder().startObject().field("time", new Date()).endObject());
        IndexResponse indexResponse = prepareIndex.get();
        client.close();
    }


    /**
     * 测试对象数据类型
     */
    @Test
    public void test10() throws IOException {
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("_all", "kit");
        SearchResponse searchResponse = client.prepareSearch("per").setTypes("person").setQuery(matchQuery).get();

        SearchHits hits = searchResponse.getHits();
        for (SearchHit temp : hits) {
            Map<String, Object> sourceAsMap = temp.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
        client.close();
    }

    /**
     * 进行sort排序操作,在es技术解析与实战的第152
     *
     * @Description:
     */
    @Test
    public void test11() {
        FieldSortBuilder fieldSort1 = SortBuilders.fieldSort("age.keyword").order(SortOrder.DESC).sortMode(SortMode.MAX);
        SearchRequestBuilder rea = client.prepareSearch("2012-02").addSort(fieldSort1).setSize(20);
        SearchResponse searchResponse = rea.get();
        SearchHits hits = searchResponse.getHits();
        for (SearchHit temp : hits) {
            System.out.println(temp.getSourceAsString());
        }
        client.close();

    }


    /**
     * 数据列过滤,_source | include | exclude
     *
     * @Description:
     */
    @Test
    public void test12() {
        SearchRequestBuilder rea = client.prepareSearch("crawler_index_2019-07-04").setFetchSource(new String[]{"articledir"}, null);
        SearchResponse searchResponse = rea.get();
        SearchHits hits = searchResponse.getHits();
        for (SearchHit temp : hits) {
            System.out.println(temp.getSourceAsMap());
        }
        client.close();
    }


    /**
     * 判断索引是否存在
     *
     * @throws ExecutionException
     * @throws InterruptedException
     * @Description:
     */
    @Test
    public void test13() throws InterruptedException, ExecutionException {
        IndicesExistsResponse indicesExistsResponse = client.admin().indices().exists(new IndicesExistsRequest().indices("crawler_in1dex_2019-07-04")).get();
        boolean exists = indicesExistsResponse.isExists();
        System.out.println(exists);
        client.close();
    }


    /**
     * 查询一个索引下面有多少条数据
     */
    @Test
    public void test14() throws InterruptedException, ExecutionException {
        String response = client.prepareSearch("crawler_index_2019-07-04")
                .setSize(0)
                .execute()
                .actionGet()
                .toString();
        JSONObject jsonObject = JSON.parseObject(response);
        System.out.println(jsonObject.get("took"));

        client.close();
    }

    /**
     * boolean(filter--must)
     */
    @Test
    public void test15() {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.matchQuery("title", "Search")).must(QueryBuilders.matchQuery("content", "Search"));
        boolQuery.filter(QueryBuilders.termQuery("status", "published")).filter(QueryBuilders.rangeQuery("publish_date").gte("2019"));
        System.out.println(boolQuery);
        SearchResponse searchResponse = client.prepareSearch("crawler_index_2019-07-04").setQuery(boolQuery).get();
        client.close();
    }

    /**
     * match_phrase_prefix
     */
    @Test
    public void test16() {
        MatchPhrasePrefixQueryBuilder slop =
                QueryBuilders.matchPhrasePrefixQuery("content", "I wanted").slop(1);
        SearchRequestBuilder rea = client.prepareSearch("2019-04").setQuery(slop);
        SearchResponse searchResponse = rea.get();
        SearchHits hits = searchResponse.getHits();
        for (SearchHit temp : hits) {
            System.out.println(temp.getSourceAsMap());
        }
        client.close();
    }

    /**
     * multi_match
     */
    @Test
    public void test17() {
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("Kendall", "name", "content");
        SearchRequestBuilder rea = client.prepareSearch("2019-04").setQuery(multiMatchQueryBuilder);
        SearchResponse searchResponse = rea.get();
        SearchHits hits = searchResponse.getHits();
        for (SearchHit temp : hits) {
            System.out.println(temp.getSourceAsMap());
        }
        client.close();
    }

    /**
     * terms
     */
    @Test
    public void test18() {
        TermsQueryBuilder multiMatchQueryBuilder = QueryBuilders.termsQuery("content", "shouts", "的");
        SearchRequestBuilder rea = client.prepareSearch("2019-04").setQuery(multiMatchQueryBuilder);
        SearchResponse searchResponse = rea.get();
        SearchHits hits = searchResponse.getHits();
        for (SearchHit temp : hits) {
            System.out.println(temp.getSourceAsMap());
        }
        client.close();
    }

    /**
     * 插入一条数据
     * @throws IOException
     */
    @Test
    public void test19() throws IOException {
        String html = "<h3>Beware of Threat Intelligence Abuse</h3>\n" +
                "<p>In addition to understanding the 就公共文化 value of threat intelligence, it’s also important to beware of “abuse” cases where threat intelligence can actually undermine incident response. For example, some organizations choose to rely simply on free threat feeds, <a href=\"https://www.recordedfuture.com/threat-intelligence-feeds/\">which should not be mistaken</a> for real threat intelligence, believing this will minimize up-front costs while still providing actionable information to help the incident response teams. </p>\n" +
                "<p>Simply bringing more data to table without the rest of the people, process, and technology elements sorted out can often add to the burden of incident responders by providing even more information and alerts for them to sort out, forcing them to manually research data that may turn out to be <a href=\"https://www.recordedfuture.com/reduce-alert-fatigue/\">false positives or irrelevant alerts</a>. To fully address the primary incident response pain points, threat intelligence needs to be comprehensive, relevant, contextualized, and integrated with other security tools.</p>\n" +
                "<p>Achieving these objectives with your threat intelligence program is critical. When an incident response team becomes more proactive and reduces reactivity, they not only contribute to the desired security posture, but they also become an integral part of improving the information security program’s top and bottom lines.</p>\n" +
                "<h3>Learn More</h3>\n" +
                "<p>For information on how to leverage effective threat intelligence to become more proactive in your vulnerability management program, <a href=\"https://go.recordedfuture.com/demo\"  rel=\"noopener noreferrer\">request a personalized demo</a> of Recorded Future today.</p>";
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().field("name", "tom").field("content", html).endObject();
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex("2019-04", "doc", "10");
        IndexRequestBuilder setSource = indexRequestBuilder.setSource(xContentBuilder);
        IndexResponse response = setSource.get();// 这个是真正的创建对象
        System.out.println(response.status());// 输出： CREATED
        client.close();
    }

    /**
     * must
     */
    @Test
    public void test20() {
        TermQueryBuilder age = QueryBuilders.termQuery("age", 88);
        MatchQueryBuilder name = QueryBuilders.matchQuery("name", "li");
        BoolQueryBuilder must = QueryBuilders.boolQuery().must(age).must(name);
        SearchResponse searchResponse = client.prepareSearch("2019-06").setQuery(must).get();
        SearchHits hits = searchResponse.getHits();
        for (SearchHit temp : hits) {
            System.out.println(temp.getSourceAsMap());
        }
        client.close();
    }

}
