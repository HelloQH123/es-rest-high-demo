package github.qh.es;

import github.qh.es.infrastructure.document.EsDeleteApiRepository;
import github.qh.es.infrastructure.document.EsExistsApiRepository;
import github.qh.es.infrastructure.document.EsGetApiRepository;
import github.qh.es.infrastructure.document.EsIndexApiRepository;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

@SpringBootTest
class EsQueryApplicationTests {

    @Resource
    private EsIndexApiRepository esIndexApiRepository;

    @Resource
    private EsGetApiRepository esGetApiRepository;

    @Resource
    private EsExistsApiRepository esExistsApiRepository;

    @Resource
    private EsDeleteApiRepository esDeleteApiRepository;

    @Test
    void contextLoads() {
        esIndexApiRepository.createIndex();
    }

    @Test
    void get(){
        esGetApiRepository.get();
    }

    @Test
    void exists(){
        esExistsApiRepository.exists();
    }

    @Test
    void delete(){
        esDeleteApiRepository.delete();
    }

}
