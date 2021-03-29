package github.qh.es.infrastructure.document;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Map;

/**
 * @author qu.hao
 * @date 2021-03-28- 12:04 下午
 * @email quhao.mi@foxmail.com
 */
@Repository
@Slf4j
public class EsGetApiRepository {

    @Resource
    private RestHighLevelClient client;

    public void get(){

        //查询的索引和id
        GetRequest request = new GetRequest(
                "index_json",
                "1");

        //设置要查询的东西
        setFetchSource(request);

        //显式地指定将返回的存储字段。默认情况下，返回_source字段 todo 不太清楚有啥作用,如果指定了这个字段，上面的setFetchSource就不起作用了，结果查询不出来任何值
        //request.storedFields("_source");

        //设置路由key, 发现输入任何的值都可以查出来，可能就是一个路由规则
        request.routing("routing1");

        //设置执行搜索的首选项。默认情况下在分片之间随机化。可以设置为_local，以偏好本地碎片或自定义值，这保证了在不同的请求中使用相同的顺序。todo 不知道啥作用
        request.preference("preference");

        //是否实时执行，默认为true
        request.realtime(false);

        //在这个get操作之前是否执行了刷新，从而导致该操作返回最新的值。注意，重get不应该将此设置为true。默认值为false。
        request.refresh(true);

        //查询的数据版本，和创建索引时候的版本关联，如果和创建时候指定的不一样就会报错
        request.version(Versions.MATCH_ANY);

        //与创建索引时候的版本类型关联,todo 不知道啥作用
        request.versionType(VersionType.EXTERNAL);

        GetResponse getResponse = null;
        try {
            getResponse = client.get(request, RequestOptions.DEFAULT);
            //异步方式获取返回值
            //getAsync(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String index = getResponse.getIndex();
        String id = getResponse.getId();
        if (getResponse.isExists()) {
            long version = getResponse.getVersion();
            String sourceAsString = getResponse.getSourceAsString();
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            byte[] sourceAsBytes = getResponse.getSourceAsBytes();
            if(log.isDebugEnabled()){
                log.debug("查询出来的索引信息：{}",index);
                log.debug("查询出来的id信息：{}",id);
                log.debug("查询出来的版本信息：{}",version);
                log.debug("查询出来的数据信息：{}（字符串）",sourceAsString);
                log.debug("查询出来的数据信息：{}（map）",sourceAsMap);
                log.debug("查询出来的数据信息：{}（byte）",sourceAsBytes);
            }
        } else {
            log.info("没有查到数据");
        }
    }

    private void getAsync(GetRequest request) {
        ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                log.info("onResponse");
            }

            @Override
            public void onFailure(Exception e) {
                log.info("onFailure");
            }
        };

        client.getAsync(request,RequestOptions.DEFAULT,listener);
    }

    private void setFetchSource(GetRequest request) {
        //自己指定查询字段
        {
            String[] includes = new String[]{"user", "message"};
            String[] excludes = Strings.EMPTY_ARRAY;
            //此时查询回来的字段包含选中的字段（includes）
            FetchSourceContext fetchSourceContext =
                    new FetchSourceContext(true, includes, excludes);
            request.fetchSourceContext(fetchSourceContext);
        }
        {String[] includes = Strings.EMPTY_ARRAY;
            String[] excludes = new String[]{"message"};
            //此时查询回来的字段包含除了message字段的其他字段
            FetchSourceContext fetchSourceContext =
                    new FetchSourceContext(true, includes, excludes);
            request.fetchSourceContext(fetchSourceContext);
        }
        //不查任何字段
        request.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
        //查询全部字段
        request.fetchSourceContext(FetchSourceContext.FETCH_SOURCE);
    }
}
