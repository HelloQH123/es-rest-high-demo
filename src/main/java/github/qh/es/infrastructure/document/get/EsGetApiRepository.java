package github.qh.es.infrastructure.document.get;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.io.IOException;

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
        GetRequest request = new GetRequest(
                "posts",
                "1");

        request.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);


        String[] includes = new String[]{"message", "*Date"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);

        String[] includes1 = Strings.EMPTY_ARRAY;
        String[] excludes1 = new String[]{"message"};
        FetchSourceContext fetchSourceContext1 =
                new FetchSourceContext(true, includes1, excludes1);


        request.fetchSourceContext(fetchSourceContext1);

        request.storedFields("message");

        request.routing("routing");

        request.preference("preference");

        request.realtime(false);

        request.refresh(true);

        request.version(2);

        request.versionType(VersionType.EXTERNAL);


        GetResponse getResponse = null;
        try {
            getResponse = client.get(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String message = getResponse.getField("message").getValue();
    }
}
