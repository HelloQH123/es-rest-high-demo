package github.qh.es;

import github.qh.es.infrastructure.EsSearchRepository;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

@SpringBootTest
class EsQueryApplicationTests {

    @Resource
    private EsSearchRepository esSearchRepository;

    @Test
    void contextLoads() {
        esSearchRepository.createIndex();
    }

}
