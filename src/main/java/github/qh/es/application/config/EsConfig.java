package github.qh.es.application.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author qu.hao
 * @date 2021年03月27日 4:29 下午
 */
@Configuration
public class EsConfig {

    @Value("${es.host:localhost}")
    private String host;

    @Value("${es.port:9200}")
    private Integer port;

    @Bean
    @ConditionalOnMissingBean
    public RestHighLevelClient restHighLevelClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port, "http")
                )
        );
    }
}
