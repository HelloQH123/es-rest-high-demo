package github.qh.es.infrastructure.document;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.lucene.uid.Versions.MATCH_ANY;

/**
 * @author qu.hao
 * @date 2021年03月27日 4:38 下午
 */
@Repository
@Slf4j
public class EsIndexApiRepository {

    @Resource
    private RestHighLevelClient client;

    protected static final RequestOptions COMMON_OPTIONS;

    static {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();

        // 默认缓冲限制为100MB，此处修改为30MB。
        builder.setHttpAsyncResponseConsumerFactory(new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(30 * 1024 * 1024));
        COMMON_OPTIONS = builder.build();
    }

    /**
     * 创建索引
     */
    public void createIndex() {
        IndexRequest simpleIndex = getSimpleIndexByJsonStr();
        IndexRequest simpleIndex2 = getSimpleIndexByMap();
        IndexRequest simpleIndexByContent = getSimpleIndexByContent();
        IndexRequest simpleIndexByJson2 = getSimpleIndexByJson2();

        ArrayList<IndexRequest> indexList = Lists.newArrayList(simpleIndex,
                simpleIndex2,
                simpleIndexByContent,
                simpleIndexByJson2
        );
        try {
            indexList.forEach(
                    var -> {
                        try {
                            //同步方式创建
                            IndexResponse indexResponse = client.index(var, COMMON_OPTIONS);
                            if(log.isDebugEnabled()){
                                logResponse(var,indexResponse);
                            }
                            //异步方式创建
                            //createAsync(var);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
            );

        } finally {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void logResponse(IndexRequest request, IndexResponse indexResponse) {
        String index = indexResponse.getIndex();
        String id = indexResponse.getId();
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            log.debug("DocWriteResponse.Result.CREATED");
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            log.debug("DocWriteResponse.Result.UPDATED");
        }
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            log.debug("分片总数：{},成功分片数：{}",shardInfo.getTotal(),shardInfo.getSuccessful());
        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure :
                    shardInfo.getFailures()) {
                String reason = failure.reason();
                log.debug("失败分片：{}",reason);
            }
        }
        log.debug("创建索引index:{},id:{},创建参数：{},返回结果:{}",index,id,request,indexResponse);
    }


    /**
     * 一个最简单的索引结构（json格式）
     * 索引的基本三要素：
     * 1.索引名称（simple）
     * 2.索引id(1)
     * 3.内容源(jsonString)
     *
     * @return 索引
     */
    private IndexRequest getSimpleIndexByJsonStr() {
        IndexRequest simpleIndex = new IndexRequest("index_json");
        simpleIndex.id("1");
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        simpleIndex.source(jsonString, XContentType.JSON);
        //控制分片陆游的规则，使用此值对分片进行散列
        simpleIndex.routing("routing");
        //等待住分片的超时时间（两种方式）
        simpleIndex.timeout(TimeValue.timeValueSeconds(1));
        simpleIndex.timeout("1s");
        /*
          刷新数据策略，es的写入数据并不一定会被实时查询出啊来，需要进行数据的刷新
          为了避免上述情况，有三种刷新策略可选
          1。NONE （默认的）不刷新 请求向ElasticSearch提交了数据，不关系数据是否已经完成刷新，直接结束请求 优点：操作延时短、资源消耗低 缺点：实时性低
          2。IMMEDIATE 强制刷新 请求向ElasticSearch提交了数据，立即进行数据刷新，然后再结束请求 优点：实时性高、操作延时短 缺点：资源消耗高
          3。WAIT_UNTIL 请求向ElasticSearch提交了数据，等待数据完成刷新，然后再结束请求 优点：实时性高、资源消耗低 缺点：操作延时长
         */
        simpleIndex.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        simpleIndex.setRefreshPolicy("wait_for");
        /*
        版本号
        每个索引文档都有一个版本号。关联的版本号作为对索引API请求的响应的一部分返回。
        索引请求如果指定了版本号这个参数（IndexRequest#version)时，索引API可选择性地允许乐观并发控制机制，
        所谓乐观并发控制就是如果待操作的索引文档的版本号如果与IndexRequest#version版本不相同，则本次操作失败。
        版本控制完全是实时的，如果未提供版本，则无需验证版本信息而立即执行
         */
        simpleIndex.version(MATCH_ANY);
        /*
        指定版本类型
        ElasticSearch支持如下版本类型：
        internal（默认）
        内部版本号，只有当请求版本号与数据版本号想等时才可以执行对应的动作。
        external or external_gt
        默认外部版本号，当请求版本号大于数据存储版本号时才可以执行对应动作，如果数据不存在，则使用指定版本号。
        external_gte
        外部版本号，当请求版本号大于等于数据存储版本号时可以执行对应动作，如果数据不存在，则使用指定版本号。
         */
        simpleIndex.versionType(VersionType.INTERNAL);
        /*
        操作类型，支持两种
        OpType.CREATE:创建一个索引，如果当前这个id已经存在了，就不能被创建（id不能为空，必须指定id，id相同时报错，只支持创建文档）
        OpType.INDEX(默认):创建一个索引，如果这个索引已经存在，就覆盖（id可以为空，不指定id时自动生成，id相同时覆盖，支持创建和更新文档）
         */
        //simpleIndex.opType("create");
        simpleIndex.opType(DocWriteRequest.OpType.INDEX);
        //设置管道名称，todo 具体啥作用有待研究，网上目前没有太好的解释
        //simpleIndex.setPipeline("pipeline");


        return simpleIndex;
    }


    private IndexRequest getSimpleIndexByJson2() {
        return new IndexRequest("index_json2")
                .id("1")
                .source("user", "kimchy",
                        "postDate", new Date(),
                        "message", "trying out Elasticsearch");
    }

    /**
     * map形式的索引
     *
     * @return 索引
     */
    private IndexRequest getSimpleIndexByMap() {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "kimchy");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        return new IndexRequest("index_map")
                .id("1").source(jsonMap);
    }

    /**
     * XContentBuilder 方式创建索引
     *
     * @return 索引
     */
    private IndexRequest getSimpleIndexByContent() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("user", "kimchy");
            builder.timeField("postDate", new Date());
            builder.field("message", "trying out Elasticsearch");
            builder.endObject();
            return new IndexRequest("index_content")
                    .id("1").source(builder);

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    private void createAsync(IndexRequest var) {
        ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                if(log.isDebugEnabled()){
                    log.debug("异步创建成功，{}",indexResponse);
                }
            }

            @Override
            public void onFailure(Exception e) {
                log.error("异步创建失败",e);
            }
        };
        client.indexAsync(var, RequestOptions.DEFAULT, listener);
    }
}
