------------------------------01--------------------------------------
GET _analyze
{
"text": "hello world, java spark",
"analyzer": "standard"
}

--------------------------------------------------------------------

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

------------------------------------------
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

----------------------------------------------------------------------------------------