package com.doublew.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

public class ElasticSearchClientTest {

    private TransportClient client;

    @Before
    public void init() throws Exception {
        //创建一个Settings对象
        Settings settings = Settings.builder()
                .put("cluster.name", "my-elasticsearch")
                .build();
        //创建一个TransPortClient对象
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));

    }

    @Test
    //创建索引
    public void createIndex() throws Exception{
        // 创建Client连接对象
        /*Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));*/
        //创建名称为blog2的索引
        client.admin().indices().prepareCreate("index_hello2").get();
        //释放资源
        client.close();
    }

    @Test
    //创建映射
    public void createMapping() throws Exception{
        // 创建Client连接对象
        /*Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));*/

        // 添加映射
        /**
         * 格式：
         * "mappings" : {
         "article" : {
         "dynamic" : "false",
         "properties" : {
         "id" : { "type" : "string" },
         "content" : { "type" : "string" },
         "author" : { "type" : "string" }
         }
         }
         }
         */
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("article")
                .startObject("properties")
                .startObject("id")
                .field("type", "integer").field("store", "yes")
                .endObject()
                .startObject("title")
                .field("type", "string").field("store", "yes").field("analyzer", "ik_smart")
                .endObject()
                .startObject("content")
                .field("type", "string").field("store", "yes").field("analyzer", "ik_smart")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        // 创建映射
        PutMappingRequest mapping = Requests.putMappingRequest("index_hello")
                .type("article").source(builder);
        client.admin().indices().putMapping(mapping).get();
        /*client.admin().indices().preparePutMapping("index_hello")
                .setType("article").setSource(builder).get()*/
        //释放资源
        client.close();
    }

    @Test
    //创建文档(通过XContentBuilder)
    public void createDocument() throws Exception{
        // 创建Client连接对象
        /*Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));*/

        //创建文档信息
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id", 2)
                .field("title", "ElasticSearch是一个基于Lucene的搜索服务器")
                .field("content",
                        "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。")
                .endObject();

        // 建立文档对象
        /**
         * 参数一blog1：表示索引对象
         * 参数二article：类型
         * 参数三1：建立id
         */
        client.prepareIndex("index_hello", "article", "2").setSource(builder).get();

        //释放资源
        client.close();
    }

    @Test
    //创建文档(通过实体转json)
    public void createDocumentJackson() throws Exception{
        // 创建Client连接对象
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));

        // 描述json 数据
        //{id:xxx, title:xxx, content:xxx}
        Article article = new Article();
        article.setId(3);
        article.setTitle("搜索工作其实很快乐");
        article.setContent("我们希望我们的搜索解决方案要快，我们希望有一个零配置和一个完全免费的搜索模式，我们希望能够简单地使用JSON通过HTTP的索引数据，我们希望我们的搜索服务器始终可用，我们希望能够一台开始并扩展到数百，我们要实时搜索，我们要简单的多租户，我们希望建立一个云的解决方案。Elasticsearch旨在解决所有这些问题和更多的问题。");

        ObjectMapper objectMapper = new ObjectMapper();

        // 建立文档
        client.prepareIndex("blog2", "article", String.valueOf(article.getId()))
                //.setSource(objectMapper.writeValueAsString(article)).get();
                .setSource(objectMapper.writeValueAsString(article).getBytes(), XContentType.JSON).get();

        //释放资源
        client.close();
    }

    @Test
    public void testIdQuery() throws Exception {
        //client对象为TransportClient对象
        SearchResponse response = client.prepareSearch("index_hello")
                .setTypes("article")
                //设置要查询的id
                .setQuery(QueryBuilders.idsQuery().addIds("1","2"))
                //执行查询
                .get();
        //取查询结果
        SearchHits searchHits = response.getHits();
        //取查询结果总记录数
        System.out.println(searchHits.getTotalHits());
        Iterator<SearchHit> hitIterator = searchHits.iterator();
        while(hitIterator.hasNext()) {
            SearchHit searchHit = hitIterator.next();
            Map<String, Object> document = searchHit.getSourceAsMap();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
            //打印整行数据
            System.out.println(searchHit.getSourceAsString());
        }
    }

    @Test
    public void testTermQuery() throws Exception{
        //2、设置搜索条件
        SearchResponse searchResponse = client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(QueryBuilders.termQuery("content", "搜索")).get();

        //3、遍历搜索结果数据
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
            System.out.println("title:" + searchHit.getSource().get("title"));
        }

        //4、释放资源
        client.close();
    }

    @Test
    public void testStringQuery() throws Exception{
        //2、设置搜索条件
        SearchResponse searchResponse = client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(QueryBuilders.queryStringQuery("搜索")).get();

        //3、遍历搜索结果数据
        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
            System.out.println("title:" + searchHit.getSource().get("title"));
        }

        //4、释放资源
        client.close();
    }

    @Test
    //批量插入100条数据
    public void testMutilAdd() throws Exception{
        ObjectMapper objectMapper = new ObjectMapper();

        for (int i = 4; i <= 100; i++) {
            // 描述json 数据
            Article article = new Article();
            article.setId(i);
            article.setTitle(i + "搜索工作其实很快乐");
            article.setContent(i
                    + "我们希望我们的搜索解决方案要快，我们希望有一个零配置和一个完全免费的搜索模式，我们希望能够简单地使用JSON通过HTTP的索引数据，我们希望我们的搜索服务器始终可用，我们希望能够一台开始并扩展到数百，我们要实时搜索，我们要简单的多租户，我们希望建立一个云的解决方案。Elasticsearch旨在解决所有这些问题和更多的问题。");

            // 建立文档
            client.prepareIndex("index_hello", "article", String.valueOf(article.getId()))
                    //.setSource(objectMapper.writeValueAsString(article)).get();
                    .setSource(objectMapper.writeValueAsString(article).getBytes(),XContentType.JSON).get();
        }

        //释放资源
        client.close();
    }

    @Test
    //分页查询
    public void testFenYe() throws Exception{

        // 搜索数据
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("index_hello").setTypes("article")
                .setQuery(QueryBuilders.matchAllQuery());//默认每页10条记录

        // 查询第2页数据，每页20条
        //setFrom()：从第几条开始检索，默认是0。
        //setSize():每页最多显示的记录数。
        searchRequestBuilder.setFrom(0).setSize(5);
        SearchResponse searchResponse = searchRequestBuilder.get();

        SearchHits hits = searchResponse.getHits(); // 获取命中次数，查询结果有多少对象
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit searchHit = iterator.next(); // 每个查询对象
            System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
            System.out.println("id:" + searchHit.getSource().get("id"));
            System.out.println("title:" + searchHit.getSource().get("title"));
            System.out.println("content:" + searchHit.getSource().get("content"));
            System.out.println("-----------------------------------------");
        }

        //释放资源
        client.close();
    }

    @Test
    //高亮查询
    public void testGaoLiang() throws Exception{

        //设置高亮数据
        HighlightBuilder hiBuilder=new HighlightBuilder();
        hiBuilder.preTags("<font style='color:red'>");
        hiBuilder.postTags("</font>");
        hiBuilder.field("title");
        // 搜索数据
        SearchRequestBuilder searchRequestBuilder = client
                .prepareSearch("index_hello").setTypes("article")
                .setQuery(QueryBuilders.termQuery("title", "搜索"))
                .highlighter(hiBuilder);

        //获得查询结果数据
        SearchResponse searchResponse = searchRequestBuilder.get();

        //获取查询结果集
        SearchHits searchHits = searchResponse.getHits();
        System.out.println("共搜到:"+searchHits.getTotalHits()+"条结果!");
        //遍历结果
        for(SearchHit hit:searchHits){
            System.out.println("String方式打印文档搜索内容:");
            System.out.println(hit.getSourceAsString());
            System.out.println("Map方式打印高亮内容");
            System.out.println(hit.getHighlightFields());

            System.out.println("遍历高亮集合，打印高亮片段:");
            Text[] text = hit.getHighlightFields().get("title").getFragments();
            for (Text str : text) {
                System.out.println(str);
            }
        }

        //释放资源
        client.close();
    }
}
