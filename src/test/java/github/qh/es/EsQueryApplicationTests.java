package github.qh.es;

import github.qh.es.infrastructure.document.index.EsIndexApiRepository;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

@SpringBootTest
class EsQueryApplicationTests {

    @Resource
    private EsIndexApiRepository esIndexApiRepository;

    @Test
    void contextLoads() {
        esIndexApiRepository.createIndex();
    }

}
