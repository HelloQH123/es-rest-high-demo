package github.qh.es.infrastructure.search;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author qu.hao
 * @date 2021-04-29- 3:32 下午
 * @email quhao.mi@foxmail.com
 * 终于到重点了
 * SearchRequest用于与搜索文档、聚合、建议相关的任何操作，还提供了请求对结果文档进行高亮显示的方法。
 */
@Repository
@Slf4j
public class SearchApiRepository {

    @Resource
    private RestHighLevelClient client;

    public void query(){
        //在最基本的形式中，我们可以向请求添加一个查询:
        //创建一个查询请求
        SearchRequest searchRequest = new SearchRequest();
        /*
        设置查询的索引
        SearchRequest searchRequest = new SearchRequest("posts");
         */

        /*
        设置indicatesoptions控制如何解析不可用的索引以及如何展开通配符表达
        翻译成人话呢就是去查那些索引，这个规则可以控制以通配符等规则去匹配要搜索的索引
        LENIENT_EXPAND_OPEN
        (EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE), EnumSet.of(WildcardStates.OPEN))
        主要代表如下几层意义：
        1、允许索引不存在，指定一个不存在的索引，也不会抛出异常。
        2、通配符作用范围为OPEN状态的索引。
        3、如果使用通配符来查找索引，未匹配到任何索引不会抛出异常
        具体参考 ·https://www.cnblogs.com/libin2015/p/12614032.html·
         */
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());

        //设置偏好分片，优先去那个分片查询数据，默认是随机的
        searchRequest.preference("_local");

        //设置查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //单独的查查询条件
        TermQueryBuilder query = QueryBuilders.termQuery("user", "kimchy");
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("user", "kimchy");
        matchQueryBuilder.fuzziness(Fuzziness.AUTO);
        matchQueryBuilder.prefixLength(3);
        matchQueryBuilder.maxExpansions(10);

        QueryBuilder matchQueryBuilder1 = QueryBuilders.matchQuery("user", "kimchy")
                .fuzziness(Fuzziness.AUTO)
                .prefixLength(3)
                .maxExpansions(10);
        //设置查询条件
        sourceBuilder.query(query);
        sourceBuilder.query(matchQueryBuilder);
        sourceBuilder.query(matchQueryBuilder1);

        sourceBuilder.from(0);
        sourceBuilder.size(5);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(sourceBuilder);

        //设置排序规则
        sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        sourceBuilder.sort(new FieldSortBuilder("_id").order(SortOrder.ASC));

        sourceBuilder.fetchSource(false);

        String[] includeFields = new String[] {"title", "innerObject.*"};
        String[] excludeFields = new String[] {"user"};
        sourceBuilder.fetchSource(includeFields, excludeFields);

        //高亮查询
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field highlightTitle =
                new HighlightBuilder.Field("title");
        highlightTitle.highlighterType("unified");
        highlightBuilder.field(highlightTitle);
        HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("user");
        highlightBuilder.field(highlightUser);
        searchSourceBuilder.highlighter(highlightBuilder);

        //聚合查询
        SearchSourceBuilder aggregationSearchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_company")
                .field("company.keyword");
        aggregation.subAggregation(AggregationBuilders.avg("average_age")
                .field("age"));
        aggregationSearchSourceBuilder.aggregation(aggregation);

        //类似于浏览器那种搜索提示
        SearchSourceBuilder searchSourceBuilder2 = new SearchSourceBuilder();
        TermSuggestionBuilder termSuggestionBuilder =
                SuggestBuilders.termSuggestion("user").text("kmichy");
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("suggest_user", termSuggestionBuilder);
        searchSourceBuilder2.suggest(suggestBuilder);

        searchSourceBuilder.profile(true);

        ActionListener<SearchResponse> listener = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                log.info("search success {}",searchResponse);
            }

            @Override
            public void onFailure(Exception e) {
                log.error("search file",e);
            }
        };
        try {
            client.searchAsync(searchRequest, RequestOptions.DEFAULT, listener);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            RestStatus status = searchResponse.status();
            TimeValue took = searchResponse.getTook();
            Boolean terminatedEarly = searchResponse.isTerminatedEarly();
            boolean timedOut = searchResponse.isTimedOut();
            int totalShards = searchResponse.getTotalShards();
            int successfulShards = searchResponse.getSuccessfulShards();
            int failedShards = searchResponse.getFailedShards();
            for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
                // failures should be handled here
            }
            SearchHits hits = searchResponse.getHits();
            TotalHits totalHits = hits.getTotalHits();
            // the total number of hits, must be interpreted in the context of totalHits.relation
            long numHits = totalHits.value;
            // whether the number of hits is accurate (EQUAL_TO) or a lower bound of the total (GREATER_THAN_OR_EQUAL_TO)
            TotalHits.Relation relation = totalHits.relation;
            float maxScore = hits.getMaxScore();

            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                // do something with the SearchHit
                String index = hit.getIndex();
                String id = hit.getId();
                float score = hit.getScore();
                String sourceAsString = hit.getSourceAsString();
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                String documentTitle = (String) sourceAsMap.get("title");
                List<Object> users = (List<Object>) sourceAsMap.get("user");
                Map<String, Object> innerObject =
                        (Map<String, Object>) sourceAsMap.get("innerObject");
            }

            SearchHits hits1 = searchResponse.getHits();
            for (SearchHit hit : hits1.getHits()) {
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                HighlightField highlight = highlightFields.get("title");
                Text[] fragments = highlight.fragments();
                String fragmentString = fragments[0].string();
            }

            Aggregations aggregations = searchResponse.getAggregations();
            Terms byCompanyAggregation = aggregations.get("by_company");
            Terms.Bucket elasticBucket = byCompanyAggregation.getBucketByKey("Elastic");
            Avg averageAge = elasticBucket.getAggregations().get("average_age");
            double avg = averageAge.getValue();


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
