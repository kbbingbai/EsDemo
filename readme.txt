------------------------------01--------------------------------------
GET _analyze
{
"text": "hello world, java spark",
"analyzer": "standard"
}

-------------------------------02-------------------------------------

TermQueryBuilder age = QueryBuilders.termQuery("age", 88);
MatchQueryBuilder name = QueryBuilders.matchQuery("name", "li");
BoolQueryBuilder must = QueryBuilders.boolQuery().must(age).must(name);
SearchResponse searchResponse = client.prepareSearch("2019-06").setQuery(must).get();

查询的结构为：
GET 2019-06/_search
{
  "query": {
    "bool": {
      "must": [
        {"term": {
          "age": {
            "value": 88
          }
        }},
        {
          "match": {
            "name": "li"
          }
        }
      ]
    }
  }
}

-----------------------------------03-------------------------------------------------
BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
boolQuery.must(QueryBuilders.matchQuery("title", "Search")).must(QueryBuilders.matchQuery("content", "Search"));
boolQuery.filter(QueryBuilders.termQuery("status", "published")).filter(QueryBuilders.rangeQuery("publish_date").gte("2019"));


  "bool" : {
    "must" : [
      {
        "match" : {
          "title" : {
            "query" : "Search",
            "operator" : "OR",
            "prefix_length" : 0,
            "max_expansions" : 50,
            "fuzzy_transpositions" : true,
            "lenient" : false,
            "zero_terms_query" : "NONE",
            "boost" : 1.0
          }
        }
      },
      {
        "match" : {
          "content" : {
            "query" : "Search",
            "operator" : "OR",
            "prefix_length" : 0,
            "max_expansions" : 50,
            "fuzzy_transpositions" : true,
            "lenient" : false,
            "zero_terms_query" : "NONE",
            "boost" : 1.0
          }
        }
      }
    ],
    "filter" : [
      {
        "term" : {
          "status" : {
            "value" : "published",
            "boost" : 1.0
          }
        }
      },
      {
        "range" : {
          "publish_date" : {
            "from" : "2019",
            "to" : null,
            "include_lower" : true,
            "include_upper" : true,
            "boost" : 1.0
          }
        }
      }
    ],
    "disable_coord" : false,
    "adjust_pure_negative" : true,
    "boost" : 1.0
  }
}

-----------------------------------04-----------------------------------------------------
# 通过_source获取指定的字段
curl -XGET 'http://172.16.0.14:9200/store/books/1?_source=title'
curl -XGET 'http://172.16.0.14:9200/store/books/1?_source=title,price'
curl -XGET 'http://172.16.0.14:9200/store/books/1?_source'



GET 2019-06/_search
{
  "_source": {
    "include": ["age","name"],
    "exclude": ["name"]
  }
}
----------------------------------分组-----------------------------------------------------
(1) 求最大值 最小值 平均值 和
GET 2019-06/_search
GET 2019-06/_search
{
  "size": 0,
  "aggs": {
    "age_of_min": {
      "max": {
        "field": "age"
      }
    }
  }
}

其中min,sum,avg就不通通列举了。
返回值
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
    "total": 13,
    "max_score": 0,
    "hits": []
  },
  "aggregations": {
    "age_of_min": {
      "value": 88
    }
  }
}

(2) 求基数  就是在一个字段中有几个不同的值

GET 2019-06/_search
{
  "size": 0,
  "aggs": {
    "age_of_cardinality": {
      "cardinality": {
        "field": "age"
      }
    }
  }
}
返回值是：
{
  "took": 2,
  "timed_out": false,
  "_shards": {
    "total": 5,
    "successful": 5,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": 13,
    "max_score": 0,
    "hits": []
  },
  "aggregations": {
    "age_of_cardinality": {
      "value": 5
    }
  }
}

(3) 分组查询 terms

GET 2019-06/_search
{
  "size": 0,
  "aggs": {
    "age_of_group": {
      "terms": {
        "field": "age"
      }
    }
  }
}

返回的结果是：--------
{
  "took": 8,
  "timed_out": false,
  "_shards": {
    "total": 5,
    "successful": 5,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": 13,
    "max_score": 0,
    "hits": []
  },
  "aggregations": {
    "age_of_group": {
      "doc_count_error_upper_bound": 0,
      "sum_other_doc_count": 0,
      "buckets": [
        {
          "key": 1,
          "doc_count": 7
        },
        {
          "key": 34,
          "doc_count": 3
        },
        {
          "key": 22,
          "doc_count": 1
        },
        {
          "key": 56,
          "doc_count": 1
        },
        {
          "key": 88,
          "doc_count": 1
        }
      ]
    }
  }
}

---------------------------------复杂的聚合------------------------------------
对那些有唱歌兴趣的人进行年龄分组，并求每一组的平均值
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