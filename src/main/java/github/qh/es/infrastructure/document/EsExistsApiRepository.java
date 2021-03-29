package github.qh.es.infrastructure.document;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.io.IOException;

/**
 * @author qu.hao
 * @date 2021-03-29- 3:46 下午
 * @email quhao.mi@foxmail.com
 */
@Repository
@Slf4j
public class EsExistsApiRepository {

    @Resource
    private RestHighLevelClient client;

    public void exists(){
        GetRequest getRequest = new GetRequest(
                "index_json",
                "1");
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        try {
            boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
            if(log.isDebugEnabled()){
                log.debug("查询是否存在索引->index_json:{}",exists);
            }
            //异步方式
            existsAsync(getRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void existsAsync(GetRequest getRequest) throws IOException {
        ActionListener<Boolean> listener = new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean exists) {
                if(log.isDebugEnabled()){
                    if(log.isDebugEnabled()){
                        log.debug("异步方式查询是否存在索引->index_json_exists:{}",exists);
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                log.error("异步方式查询发生异常",e);
            }
        };
        getRequest.index("index_json_exists");
        client.existsAsync(getRequest, RequestOptions.DEFAULT,listener);

    }
}
