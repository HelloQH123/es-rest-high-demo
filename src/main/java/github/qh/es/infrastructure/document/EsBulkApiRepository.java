package github.qh.es.infrastructure.document;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author qu.hao
 * @date 2021-04-29- 1:51 下午
 * @email quhao.mi@foxmail.com
 * A BulkRequest can be used to execute multiple index, update and/or delete operations using a single request.
 * 一个 bulk请求可以对多个索引执行操作，包括更新删除等操作。
 */
@Repository
@Slf4j
public class EsBulkApiRepository {

    @Resource
    private RestHighLevelClient client;

    public void bulkExecute(){
        //创建一个bulk请求
        BulkRequest request = new BulkRequest();
        //要对那些索引进行操作，Bulk API只支持用JSON或SMILE编码的文档。提供任何其他格式的文件都会导致错误。
        request.add(new IndexRequest("posts").id("1")
                .source(XContentType.JSON,"field", "foo"));

        request.add(new DeleteRequest("posts", "3"));

        request.add(new UpdateRequest("posts", "2")
                .doc(XContentType.JSON,"other", "test"));

        request.add(new IndexRequest("posts").id("4")
                .source(XContentType.JSON,"field", "baz"));

        //请求超时时间
        request.timeout(TimeValue.timeValueMinutes(2));
        request.timeout("2m");

        try {
            BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);

            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                DocWriteResponse itemResponse = bulkItemResponse.getResponse();

                switch (bulkItemResponse.getOpType()) {
                    case INDEX:
                    case CREATE:
                        IndexResponse indexResponse = (IndexResponse) itemResponse;
                        log.info("index：{}",indexResponse);
                        break;
                    case UPDATE:
                        UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                        log.info("update:{}",updateResponse);
                        break;
                    case DELETE:
                        DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                        log.info("delete:{}",deleteResponse);
                        break;
                    default:
                        log.info("nil");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * BulkProcessor
     * BulkProcessor通过提供一个实用程序类来简化Bulk API的使用，该实用程序类允许索引/更新/删除操作在添加到处理器时透明地执行。
     */
    public void bulkProcessExecute(){

        /*
          处理请求回调监听
         */
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                log.debug("Executing bulk [{}] with {} requests",
                        executionId, numberOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  BulkResponse response) {
                if (response.hasFailures()) {
                    log.warn("Bulk [{}] executed with failures", executionId);
                } else {
                    log.debug("Bulk [{}] completed in {} milliseconds",
                            executionId, response.getTook().getMillis());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  Throwable failure) {
                log.error("Failed to execute bulk", failure);
            }
        };

        /*
          要操作的索引
         */
        IndexRequest one = new IndexRequest("posts").id("1")
                .source(XContentType.JSON, "title",
                        "In which order are my Elasticsearch queries executed?");
        IndexRequest two = new IndexRequest("posts").id("2")
                .source(XContentType.JSON, "title",
                        "Current status and upcoming changes in Elasticsearch");
        IndexRequest three = new IndexRequest("posts").id("3")
                .source(XContentType.JSON, "title",
                        "The Future of Federated Search in Elasticsearch");

        /*
          创建方式一
         */
        BulkProcessor.Builder builder = BulkProcessor.builder(
                (request, bulkListener) ->
                        client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                listener);
        //根据当前添加的操作数量设置刷新新批量请求的时间(默认值为1000，使用-1禁用它)
        builder.setBulkActions(500);
        //根据当前添加的操作的大小设置刷新请求的时间(默认为5Mb，使用-1禁用它)
        builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB));
        //设置允许执行的并发请求数(默认为1，使用0表示只允许执行单个请求)
        builder.setConcurrentRequests(0);
        //设置刷新间隔，如果间隔通过则刷新任何挂起的BulkRequest(默认未设置)
        builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
        //设置一个初始等待1秒并重试3次的固定回退策略。
        //更多选项请参见BackoffPolicy.noBackoff()， BackoffPolicy.constantBackoff()和BackoffPolicy.exponentialBackoff()。
        builder.setBackoffPolicy(BackoffPolicy
                .constantBackoff(TimeValue.timeValueSeconds(1L), 3));
        BulkProcessor bulkProcessor1 = builder.build();
        bulkProcessor1.add(one);
        bulkProcessor1.add(two);
        bulkProcessor1.add(three);


        /*
        方式二
        这些请求将由BulkProcessor执行，它负责调用BulkProcessor。监听每个批量请求。
        监听器提供了访问BulkRequest和BulkResponse的方法:
         */
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                (request, bulkListener) ->
                        client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                listener).build();
        bulkProcessor.add(one);
        bulkProcessor.add(two);
        bulkProcessor.add(three);

        try {
            //可用于等待所有请求都被处理或指定的等待时间:
            boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
            log.info("wait close {}",terminated);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }

        //关闭请求
        bulkProcessor.close();
    }
}
