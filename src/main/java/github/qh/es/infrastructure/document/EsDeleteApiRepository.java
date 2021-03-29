package github.qh.es.infrastructure.document;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
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
public class EsDeleteApiRepository {

    @Resource
    private RestHighLevelClient client;

    public void delete() {
        DeleteRequest request = new DeleteRequest(
                "index_map",
                "1");

        request.routing("routing");
        //设置请求超时时间
        request.timeout(TimeValue.timeValueMinutes(2));
        request.timeout("2m");

        //刷新策略，创建索引里面有介绍 github.qh.es.infrastructure.document.EsIndexApiRepository.getSimpleIndexByJsonStr
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.setRefreshPolicy("wait_for");

        //删除指定的版本
        request.version(2);
        request.versionType(VersionType.EXTERNAL);
        try {
            DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
            String index = deleteResponse.getIndex();
            String id = deleteResponse.getId();
            long version = deleteResponse.getVersion();
            ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                log.info("分片总数:{},删除分片数:{}",shardInfo.getTotal(),shardInfo.getSuccessful());
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure :
                        shardInfo.getFailures()) {
                    String reason = failure.reason();
                    log.warn("删除分片失败，失败信息：{}",reason);
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("删除的索引信息：{}",index);
                log.debug("删除的id信息：{}",id);
                log.debug("删除的版本信息：{}",version);
            }
            //异步方式
            existsAsync(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void existsAsync(DeleteRequest getRequest) throws IOException {
        ActionListener<DeleteResponse> listener = new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                if (log.isDebugEnabled()) {
                    log.debug("异步方式删除索引，返回结果:{}", deleteResponse);
                }
            }

            @Override
            public void onFailure(Exception e) {
                log.error("异步方式删除索引异常", e);
            }
        };
        getRequest.index("index_json2");
        client.deleteAsync(getRequest, RequestOptions.DEFAULT, listener);

    }
}
