import com.doublew.es.entity.Article;
import com.doublew.es.repository.ArticleRepository;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import java.util.List;
import java.util.Optional;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")
public class SpringDataElasticSearchTest {

    @Autowired
    private ArticleRepository articleRepository;

    @Autowired
    private ElasticsearchTemplate template;

    @Test
    public void createIndex() throws Exception {
        //创建索引，并配置映射关系
        template.createIndex(Article.class);
        //配置映射关系
        //template.putMapping(Article.class);
    }

    /**测试保存文档*/
    @Test
    public void saveArticle(){
        Article article = new Article();
        article.setId(3);
        article.setTitle("测试SpringData ElasticSearch3");
        article.setContent("Spring Data ElasticSearch 基于 spring data API 简化 elasticSearch操作，将原始操作elasticSearch的客户端API 进行封装 \n" +
                "    Spring Data为Elasticsearch Elasticsearch项目提供集成搜索引擎");
        articleRepository.save(article);
    }

    /**测试删除*/
    @Test
    public void delete(){
        Article article = new Article();
        article.setId(100);
        articleRepository.delete(article);
    }

    /**测试更新*/
    @Test
    public void update(){
        Article article = new Article();
        article.setId(2);
        article.setTitle("2222222222");
        article.setContent("ElasticSearch是一个基于Lucene的搜索服务器。它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口");
        articleRepository.save(article);
    }

    /**批量插入*/
    @Test
    public void save10(){
        for(int i=10;i<=20;i++){
            Article article = new Article();
            article.setId(i);
            article.setTitle(i+"elasticSearch 3.0版本发布..，更新");
            article.setContent(i+"ElasticSearch是一个基于Lucene的搜索服务器。它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口");
            articleRepository.save(article);
        }
    }

    /**查询全部*/
    @Test
    public void findAll(){
        Iterable<Article> articleRepositoryAll = articleRepository.findAll();
        articleRepositoryAll.forEach(System.out::println);
    }

    /**查询根据ID*/
    @Test
    public void findById(){
        Optional<Article> op = articleRepository.findById(1l);
        System.out.println(op.get());
    }

    @Test
    public void testFindByTitle() throws Exception {
        List<Article> list = articleRepository.findByTitle("14elasticSearch");
        list.stream().forEach(a-> System.out.println(a));
    }

    @Test
    public void testFindByTitleOrContent() throws Exception {
        Pageable pageable = PageRequest.of(1, 15);
        articleRepository.findByTitleOrContent("maven", "商务与投资", pageable)
                .forEach(a-> System.out.println(a));
    }

    @Test
    public void testNativeSearchQuery() throws Exception {
        //创建一个查询对象
        NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.queryStringQuery("14elasticSearch").defaultField("title"))
                .withPageable(PageRequest.of(0, 15))
                .build();
        //执行查询
        List<Article> articleList = template.queryForList(query, Article.class);
        articleList.forEach(a-> System.out.println(a));
    }








}
