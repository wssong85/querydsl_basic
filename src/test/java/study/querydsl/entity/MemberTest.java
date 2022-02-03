package study.querydsl.entity;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.QMemberDto;
import study.querydsl.dto.UserDto;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceUnit;
import javax.transaction.Transactional;

import java.util.List;

import static com.querydsl.jpa.JPAExpressions.*;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;
import static study.querydsl.entity.QMember.*;
import static study.querydsl.entity.QTeam.team;

@SpringBootTest
@Transactional
//@Commit
class MemberTest {

  @PersistenceContext
  EntityManager em;

  JPAQueryFactory queryFactory;

  @BeforeEach
  public void before() {
    queryFactory = new JPAQueryFactory(em);
    Team teamA = new Team("teamA");
    Team teamB = new Team("teamB");
    em.persist(teamA);
    em.persist(teamB);

    Member member1 = new Member("member1", 10, teamA);
    Member member2 = new Member("member2", 20, teamA);

    Member member3 = new Member("member3", 30, teamB);
    Member member4 = new Member("member4", 40, teamB);

    em.persist(member1);
    em.persist(member2);
    em.persist(member3);
    em.persist(member4);

    em.flush();
    em.clear();

    List<Member> members = em.createQuery("select m from Member m", Member.class)
            .getResultList();

    for (Member member : members) {
      System.out.println("member = " + member);
      System.out.println("-> member.team = " + member.getTeam());
    }
  }

  @Test
  public void startJPQL() {
    //member1을 찾아라
    String qlString = "select m from Member m where m.username = :username";
    Member findMember = em.createQuery(qlString, Member.class)
            .setParameter("username", "member1")
            .getSingleResult();
//    assertThat(findMember.getUsername()).isEqualTo("member1");
    assertEquals(findMember.getUsername(), "member1");
  }

  @Test
  public void startQuqerydsl() {
//    QMember m = new QMember("m");
    Member findMember = queryFactory
            .select(member)
            .from(member)
            .where(member.username.eq("member1"))
            .fetchOne();
//    assertThat(findMember.getUsername()).isEqualTo("member1");
    assertEquals(findMember.getUsername(), "member1");
  }

  @Test
  public void search() {
    Member findMember = queryFactory
            .selectFrom(member)
            .where(member.username.eq("member1")
                    .and(member.age.eq(10)))
            .fetchOne();

    assertEquals(findMember.getUsername(), "member1", "member1이 있는지 확인");
  }

  @Test
  public void searchAndParam() {
    Member findMember = queryFactory
            .selectFrom(member)
            .where(
                    member.username.eq("member1"),
                    member.age.between(10, 30)
            )
            .fetchOne();

    assertEquals(findMember.getUsername(), "member1", "member1이 있는지 확인");
  }

  @Test
  public void resultFetch() {
//    List<Member> fetch = queryFactory
//            .selectFrom(member)
//            .fetch();
//    Member fetchOne = queryFactory
//            .selectFrom(member)
//            .fetchOne();
//    Member fetchFirst = queryFactory
//            .selectFrom(QMember.member)
//            .fetchFirst();
//    QueryResults<Member> results = queryFactory
//            .selectFrom(member)
//            .fetchResults();
//    results.getTotal();
//    List<Member> contents = results.getResults();

    long fetchCount = queryFactory
            .selectFrom(member)
            .fetchCount();
  }

  /**
   * 회원 정렬 순서
   * 1. 회원 나이 내림차순(desc)
   * 2. 회원 이름 올림차순(asc)
   * 단 2에서 회원 이름이 없으면 마지막에 출력(nulls last)
   */
  @Test
  public void sort() {
    em.persist(new Member(null, 100));
    em.persist(new Member("member5", 100));
    em.persist(new Member("member6", 100));

    List<Member> result = queryFactory
            .selectFrom(member)
            .orderBy(member.age.desc(), member.username.asc().nullsLast())
            .fetch();

    Member member5 = result.get(0);
    Member member6 = result.get(1);
    Member memberNull = result.get(2);

    assertEquals(member5.getUsername(), "member5");
    assertEquals(member6.getUsername(), "member6");
    assertEquals(memberNull.getUsername(), null);
  }

  @Test
  public void paging1() {
    List<Member> result = queryFactory
            .selectFrom(member)
            .orderBy(member.username.desc())
            .offset(0)
            .limit(2)
            .fetch();

    assertEquals(result.size(), 2);
  }

  @Test
  public void paging2() {
    QueryResults<Member> queryResults = queryFactory
            .selectFrom(member)
            .orderBy(member.username.desc())
            .offset(1)
            .limit(2)
            .fetchResults();

    assertEquals(queryResults.getTotal(), 4);
    assertEquals(queryResults.getLimit(), 2);
    assertEquals(queryResults.getOffset(), 1);
    assertEquals(queryResults.getResults().size(), 2);
  }

  @Test
  public void aggregation() {
    List<Tuple> result = queryFactory
            .select(
                    member.count(),
                    member.age.sum(),
                    member.age.avg(),
                    member.age.max(),
                    member.age.min()
            )
            .from(member)
            .fetch();

    Tuple tuple = result.get(0);

    assertEquals(tuple.get(member.count()), 4);
    assertEquals(tuple.get(member.age.sum()), 100);
    assertEquals(tuple.get(member.age.avg()), 25);
    assertEquals(tuple.get(member.age.max()), 40);
    assertEquals(tuple.get(member.age.min()), 10);
  }

  /**
   * 팀의 이름과 각 팀의 평균 연령을 구해라.
   */
  @Test
  public void group() throws Exception {
    List<Tuple> result = queryFactory
            .select(team.name, member.age.avg())
            .from(member)
            .join(member.team, team)
            .groupBy(team.name)
            .fetch();

    Tuple teamA = result.get(0);
    Tuple teamB = result.get(1);

    assertEquals(teamA.get(team.name), "teamA");
    assertEquals(teamA.get(member.age.avg()), 15); // 10+20 / 2

    assertEquals(teamB.get(team.name), "teamB");
    assertEquals(teamB.get(member.age.avg()), 35); // 30+40 / 2

  }

  /**
   * 팀 A에 소속된 모든 회원
   */
  @Test
  public void join() {
    List<Member> result = queryFactory
            .selectFrom(member)
            .join(member.team, team)
//            .leftJoin(member.team, team)
//            .on(member.team.id.eq(team.id))
            .where(team.name.eq("teamA"))
            .fetch();

    assertThat(result)
            .extracting("username")
            .containsExactly("member1", "member2");

  }

  /**
   * 세타 조인
   * 회원의 이름이 팀 이름과 같은 회원 조회
   */
  @Test
  public void theta_join() {
    em.persist(new Member("teamA"));
    em.persist(new Member("teamB"));
    em.persist(new Member("teamC"));

    List<Member> result = queryFactory
            .select(member)
            .from(member, team)
            .where(member.username.eq(team.name))
            .fetch();

    assertThat(result)
            .extracting("username")
            .containsExactly("teamA", "teamB");
  }

  /**
   * 예) 회원과 팀을 조인하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
   * JPQL: select m, t from Member m left join m.team t with(on) t.name = 'teamA'
   */
  @Test
  public void join_on_filtering() {
    List<Tuple> result = queryFactory
            .select(member, team)
            .from(member)
            // leftJoin 의 테이블로 조건을 경우 on 절 활용
            .leftJoin(member.team, team).on(team.name.eq("teamA"))
            .fetch();

    for (Tuple tuple : result) {
      System.out.println("tuple = " + tuple);
    }

  }

  /**
   * 연관관계 없는 엔티티 외부 조인
   * 회원의 이름이 팀 이름과 같은 대상 외부 조인
   */
  @Test
  public void join_on_no_relation() {
    em.persist(new Member("teamA"));
    em.persist(new Member("teamB"));
    em.persist(new Member("teamC"));

    List<Tuple> result = queryFactory
            .select(member, team)
            .from(member)
            .leftJoin(team).on(member.username.eq(team.name))
//            .join(team).on(member.username.eq(team.name))
//            .where(member.username.eq(team.name))
            .fetch();

    for (Tuple tuple : result) {
      System.out.println("tuple = " + tuple);
    }

  }

  @PersistenceUnit
  EntityManagerFactory emf;

  @Test
  public void fetchJoinNo() {
    em.flush();
    em.clear();

    Member findMember = queryFactory
            .selectFrom(member)
            .join(member.team, team)
            .where(member.username.eq("member1"))
            .fetchOne();

    boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
    assertThat(loaded)
            .as("패치 조인 미적용")
            .isFalse();
  }

  @Test
  public void fetchJoinUse() {
    em.flush();
    em.clear();

    Member findMember = queryFactory
            .selectFrom(member)
            .join(member.team, team).fetchJoin()
            .where(member.username.eq("member1"))
            .fetchOne();

    boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
    assertThat(loaded)
            .as("패치 조인 적용")
            .isTrue();
  }

  /**
   * 서브쿼리: com.querydsl.jpa.JPAExpressions
   *
   * 나이가 가장 많은 회원 조회
   */
  @Test
  public void subQuery() {

    QMember memberSub = new QMember("memberSub");

    List<Member> result = queryFactory
            .selectFrom(member)
            .where(member.age.eq(
                    select(memberSub.age.max())
                            .from(memberSub)
            ))
            .fetch();

    assertThat(result)
            .extracting("age")
            .containsExactly(40);
  }

  /**
   * 서브쿼리: com.querydsl.jpa.JPAExpressions
   *
   * 나이가 평균 이상인 회원
   */
  @Test
  public void subQueryGoe() {

    QMember memberSub = new QMember("memberSub");

    List<Member> result = queryFactory
            .selectFrom(member)
            .where(member.age.goe(
                    select(memberSub.age.avg())
                            .from(memberSub)
            ))
            .fetch();

    assertThat(result)
            .extracting("age")
            .containsExactly(30, 40);
  }

  /**
   * 서브쿼리: com.querydsl.jpa.JPAExpressions
   *
   * 나이가 10살이상인 회원
   */
  @Test
  public void subQueryIn() {

    QMember memberSub = new QMember("memberSub");

    List<Member> result = queryFactory
            .selectFrom(member)
            .where(member.age.in(
                    select(memberSub.age)
                            .from(memberSub)
                            .where(memberSub.age.gt(10))
            ))
            .fetch();

    assertThat(result)
            .extracting("age")
            .containsExactly(20, 30, 40);
  }

  /**
   * from 절의 subquery 를 써야 한다면, native sql 을 사용한다.
   */
  @Test
  public void selectSubQuery() {

    QMember memberSub = new QMember("memberSub");

    List<Tuple> result = queryFactory
            .select(member.username,
                    select(memberSub.age.max())
                            .from(memberSub)
                            .where(memberSub.age.eq(member.age)))
            .from(member)
            .fetch();

    for (Tuple tuple : result) {
      System.out.println("tuple = " + tuple);
    }
  }

  @Test
  public void basicCase() {
    List<String> result = queryFactory
            .select(member.age
                    .when(10).then("열살")
                    .when(20).then("스무살")
                    .otherwise("기타"))
            .from(member)
            .fetch();

    for (String s : result) {
      System.out.println("s = " + s);
    }
  }

  @Test
  public void complexCase() {
    List<String> result = queryFactory
            .select(new CaseBuilder()
                    .when(member.age.between(0, 20)).then("0 ~ 20살")
                    .when(member.age.between(21, 30)).then("21살~31살")
                    .otherwise("기타"))
            .from(member)
            .fetch();

    for (String s : result) {
      System.out.println("s = " + s);
    }

  }

  @Test
  public void constant() {
    List<Tuple> result = queryFactory
            .select(member.username, Expressions.constant("A"))
            .from(member)
            .fetch();

    for (Tuple tuple : result) {
      System.out.println("tuple = " + tuple);
    }

  }

  @Test
  public void concat() {
    List<String> fetch = queryFactory
            .select(member.username.concat("_").concat(member.age.stringValue()))
            .from(member)
            .where(member.username.eq("member1"))
            .fetch();
    for (String s : fetch) {
      System.out.println("s = " + s);
    }
  }

  @Test
  public void simpleProjection() {
    List<String> result = queryFactory
            .select(member.username)
            .from(member)
            .fetch();

    for (String s : result) {
      System.out.println("s = " + s);
    }
  }

  @Test
  public void tupleProjection() {
    List<Tuple> result = queryFactory
            .select(member.username, member.age)
            .from(member)
            .fetch();

    for (Tuple tuple : result) {
      String username = tuple.get(member.username);
      System.out.println("username = " + username);
      Integer age = tuple.get(member.age);
      System.out.println("age = " + age);
    }
  }

  @Test
  public void findDtoByJPQL() {
    List<MemberDto> resultList = em.createQuery("select new study.querydsl.dto.MemberDto(m.username, m.age) from Member m", MemberDto.class)
            .getResultList();

    for (MemberDto memberDto : resultList) {
      System.out.println("memberDto = " + memberDto);
    }
  }

  @Test
  public void findDtoBySetter() {
    List<MemberDto> result = queryFactory
            .select(Projections.bean(MemberDto.class,
                    member.username,
                    member.age))
            .from(member)
            .fetch();

    for (MemberDto memberDto : result) {
      System.out.println("memberDto = " + memberDto);
    }
  }

  @Test
  public void findDtoByField() {
    List<MemberDto> result = queryFactory
            .select(Projections.fields(MemberDto.class,
                    member.username,
                    member.age))
            .from(member)
            .fetch();

    for (MemberDto memberDto : result) {
      System.out.println("memberDto = " + memberDto);
    }
  }

  @Test
  public void findDtoByContructor() {
    List<MemberDto> result = queryFactory
            .select(Projections.constructor(MemberDto.class,
                    member.username,
                    member.age))
            .from(member)
            .fetch();

    for (MemberDto memberDto : result) {
      System.out.println("memberDto = " + memberDto);
    }
  }

  @Test
  public void findUserDto() {
    QMember memberSub = new QMember("memberSub");
    List<UserDto> result = queryFactory
            .select(
                    Projections.fields(UserDto.class,
                    member.username.as("name"),
//                    ExpressionUtils.as(member.username, "name"),
                    ExpressionUtils.as(
                            JPAExpressions
                                    .select(memberSub.age.max())
                                    .from(memberSub), "age")
                    ))
            .from(member)
            .fetch();

    for (UserDto memberDto : result) {
      System.out.println("memberDto = " + memberDto);
    }
  }

  @Test
  public void findUserDtoByConstructor() {
    List<UserDto> result = queryFactory
            .select(Projections.constructor(UserDto.class,
//                    member.username,
                    member.username,
                    member.age))
            .from(member)
            .fetch();

    for (UserDto userDto : result) {
      System.out.println("userDto = " + userDto);
    }
  }

  @Test
  public void findDtoByQueryProjection() {
    List<MemberDto> result = queryFactory
            .select(new QMemberDto(member.username, member.age))
            .from(member)
            .fetch();

    for (MemberDto memberDto : result) {
      System.out.println("memberDto = " + memberDto);
    }
  }

  @Test
  public void dynamicQuery_BooleanBUilder() {
    String usernameParam = "member1";
    Integer ageParam = 10;

    List<Member> result = searchMember1(usernameParam, ageParam);
    assertThat(result.size()).isEqualTo(1);
  }

  private List<Member> searchMember1(String usernameCond, Integer ageCond) {

    BooleanBuilder builder = new BooleanBuilder();
//    BooleanBuilder builder = new BooleanBuilder(member.username.eq(usernameCond));
    if (usernameCond != null) {
      builder.and(member.username.eq(usernameCond));
    }

    if (ageCond != null) {
      builder.and(member.age.eq(ageCond));
    }

    return queryFactory
            .selectFrom(member)
            .where(builder)
            .fetch();
  }

  @Test
  public void dynamicQuery_WhereParam() {
    String usernameParam = "member1";
    Integer ageParam = null;

    List<Member> result = searchMember2(usernameParam, ageParam);
    assertThat(result.size()).isEqualTo(1);
  }

  private List<Member> searchMember2(String usernameCond, Integer ageCond) {
    return queryFactory
            .selectFrom(member)
//            .where(usernameEq(usernameCond), ageEq(ageCond))
            .where(allEq(usernameCond, ageCond))
            .fetch();
  }

  private BooleanExpression usernameEq(String usernameCond) {
    return usernameCond != null ? member.username.eq(usernameCond) : null;
  }

  private BooleanExpression ageEq(Integer ageCond) {
    return ageCond != null ? member.age.eq(ageCond) : null;
  }

  private Predicate allEq(String usernameCond, Integer ageCond) {
    return usernameEq(usernameCond).and(ageEq(ageCond));
  }

  @Test
  @Commit
  public void bulkUpdate() {
    // member1 = 10 -> DB member1
    // member2 = 20 -> DB member2
    // member3 = 30 -> DB member3
    // member4 = 40 -> DB member4

    long count = queryFactory
            .update(member)
            .set(member.username, "비회원")
            .where(member.age.lt(28))
//            .where(member.age.lt(10))
            .execute();

    // db 에 직접 수정, 삭제 쿼리 진행 시, 영속성 컨텍스트 초기화
    em.flush();
    em.clear();

    // member1 = 10 -> DB 비회원
    // member2 = 20 -> DB 비회원
    // member3 = 30 -> DB member3
    // member4 = 40 -> DB member4

    List<Member> result = queryFactory
            .selectFrom(member)
            .fetch();

    System.out.println("###");
    for (Member member1 : result) {
      System.out.println("member1 = " + member1);
    }
    System.out.println("###");

  }

  @Test
  public void bulkAdd() {
    long count = queryFactory
            .update(member)
            .set(member.age, member.age.multiply(2))
            .execute();

    em.flush();
    em.clear();

    List<Member> result = queryFactory
            .selectFrom(member)
            .fetch();

    System.out.println("###");
    for (Member member1 : result) {
      System.out.println("member1 = " + member1);
    }
    System.out.println("###");
  }

  @Test
  public void bulkDelete() {
    long count = queryFactory
            .delete(member)
            .where(member.age.gt(18))
            .execute();

    // delete 는 flush clear 안해도 인식
//    em.flush();
//    em.clear();

    System.out.println("###1");
    Member member1 = em.find(Member.class, 1L);
    System.out.println("###2");
    Member member2 = em.find(Member.class, 1L);
    System.out.println("###3");
    Member member3 = em.find(Member.class, 1L);
    System.out.println("member3 = " + member3);

//    System.out.println("###1");
//    List<Member> result = queryFactory
//            .selectFrom(member)
//            .fetch();
//
//    System.out.println("###2");
//    List<Member> result2 = queryFactory
//            .selectFrom(member)
//            .fetch();
//
//    System.out.println("###3");
//    for (Member member1 : result) {
//      System.out.println("member1 = " + member1);
//    }
//    System.out.println("###");
  }

  // 만약 user_function 을 쓸려면, 맞는 db의 dialect 에 직접 넣어줘야 함(설정포함)
  @Test
  public void sqlFunction() {
    List<String> result = queryFactory
            .select(Expressions.stringTemplate(
                            "function('concat', {0}, {1}, {2})",
                    member.username, "-1", "-2"))
            .from(member)
            .fetch();

    for (String s : result) {
      System.out.println("s = " + s);
    }
  }

  @Test
  public void sqlFunction2() {
    List<String> result = queryFactory
            .select(member.username)
            .from(member)
            .where(member.username.eq(Expressions.stringTemplate(
                    "function('lower', {0})",
                    member.username)))
            .fetch();

    for (String s : result) {
      System.out.println("s = " + s);
    }
  }

}
