package study.querydsl;

import org.hibernate.annotations.SortComparator;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.persistence.EntityManager;
import javax.transaction.Transactional;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

  @Autowired
  EntityManager em;

  @BeforeEach
  public void before() {

  }
}
