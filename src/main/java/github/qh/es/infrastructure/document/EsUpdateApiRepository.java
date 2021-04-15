package github.qh.es.infrastructure.document;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;

/**
 * @author qu.hao
 * @date 2021-03-29- 3:46 下午
 * @email quhao.mi@foxmail.com
 */
@Repository
@Slf4j
public class EsUpdateApiRepository {

    @Resource
    private RestHighLevelClient client;

    /**
     * 更新一个索引相关api
     */
    public void update() {
        //几乎万年不变的请求体，指定索引和文档id
        UpdateRequest request = new UpdateRequest(
                "posts",
                "1");

        //通过脚本方式更新
        updateWithScript(request);
        //通过文档方式更新
        updateWithDoc(request);
        //可以设置的其他参数
        setOtherParam(request);
        //执行更新操作
        try {
            //异步方式更新
            //updateByAsync(request);

            UpdateResponse updateResponse = client.update(
                    request, RequestOptions.DEFAULT);
            if(log.isDebugEnabled()){
                log.debug("更新操作返回结果：{}",updateResponse);
            }
            String index = updateResponse.getIndex();
            log.info("更新索引{}",index);
            String id = updateResponse.getId();
            log.info("id:{}",id);
            long version = updateResponse.getVersion();
            log.info("version{}",version);
            if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
                log.info("创建文档");
            } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                log.info("更新文档");
            } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
                log.info("删除文档");
            } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
                log.info("啥都没干");
            }

            // fetchSource 如果设置需要返回结果中包含内容了,如果没有设置返回内容，则result 等于null
            GetResult result = updateResponse.getGetResult();
            if (result.isExists()) {
                // 此例中如果文档不存在，且这样设置：request.scriptedUpsert(true);、request.docAsUpsert(false);，则会创建一个空内容的文档，因为脚本中没有内容，而禁止doc创建新文档
                String sourceAsString = result.sourceAsString();
                log.info("sourceAsString:{}",sourceAsString);
                Map<String, Object> sourceAsMap = result.sourceAsMap();
                log.info("sourceAsMap:{}",sourceAsMap);
                byte[] sourceAsBytes = result.source();
                log.info("sourceAsBytes:{}",sourceAsBytes);
            } else {
                log.info("无返回内容");
            }

            ReplicationResponse.ShardInfo shardInfo = updateResponse.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                log.info("分片总数：{},成功执行分片数量：{}",shardInfo.getTotal(),shardInfo.getSuccessful());
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure :
                        shardInfo.getFailures()) {
                    String reason = failure.reason();
                    log.warn("执行失败分片：{}",reason);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void updateByAsync(UpdateRequest request) {
        ActionListener<UpdateResponse> listener = new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        client.updateAsync(request, RequestOptions.DEFAULT, listener);
    }

    private void setOtherParam(UpdateRequest request) {
        //路由参数
        request.routing("routing");
        //等待主分片的超时时间
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");
        //刷新策略，前面index那章有介绍
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.setRefreshPolicy("wait_for");
        //如果要更新的文档已被更新操作的get和索引阶段之间的另一个操作更改，那么重试更新操作的次数是多少
        request.retryOnConflict(3);
        //是否返回响应
        request.fetchSource(true);
        //返回的字段
        String[] includes = new String[]{"updated", "r*"};
        String[] excludes = Strings.EMPTY_ARRAY;
        request.fetchSource(
                new FetchSourceContext(true, includes, excludes));
        //乐观锁 if_seq_no 和 if_primary_term 是用来并发控制，他们和version不同，version属于当个文档，而seq_no属于整个index
        request.setIfSeqNo(2L);
        request.setIfPrimaryTerm(1L);
        //这个更新是否应该尝试检测它是否是noop?默认值为true。
        //如果doc中定义的部分与现在的文档相同，则默认不会执行任何动作。设置detect_noop=false，就会无视是否修改，强制合并到现有的文档
        request.detectNoop(false);
        //指示无论文档是否存在，脚本都必须运行，也就是说，如果文档不存在，脚本会负责创建文档。
        request.scriptedUpsert(true);
        //文档如果不存在，则必须将部分文档用作upsert文档，是否使用saveOrUpdate模式
        request.docAsUpsert(true);
        //等待分片成功更新的数量
        request.waitForActiveShards(2);
        //等待所有分片更新成功
        request.waitForActiveShards(ActiveShardCount.ALL);
    }

    private void updateWithDoc(UpdateRequest request) {
        //通过json方式更新
        String jsonString = "{" +
                "\"updated\":\"2017-01-01\"," +
                "\"reason\":\"daily update\"" +
                "}";
        request.doc(jsonString, XContentType.JSON);
        //通过map方式更新
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("updated", new Date());
        jsonMap.put("reason", "daily update");
        request.doc(jsonMap);
        //通过XContentBuilder更新
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.timeField("updated", new Date());
                builder.field("reason", "daily update");
            }
            builder.endObject();
            request.doc(builder);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updateWithScript(UpdateRequest request) {
        //一个更新脚本，脚本的参数
        Map<String, Object> parameters = singletonMap("count", 4);
        //真正的脚本，使用·painless·语言，这里暂时搞不太懂，反正就是一个条件
        Script inline = new Script(ScriptType.INLINE, "painless",
                "ctx._source.field += params.count", parameters);
        request.script(inline);
        //另外一种类型的参数·STORED·,和上面的区别是上面的是内连脚本，每次都会被实时编译，而这种类型的脚本会被缓存
        Script stored = new Script(
                ScriptType.STORED, null, "increment-field", parameters);
        request.script(stored);
    }

}
