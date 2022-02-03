package study.querydsl.repository;

import com.querydsl.core.QueryResults;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.jpa.JPQLQuery;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.support.QuerydslRepositorySupport;
import org.springframework.data.repository.support.PageableExecutionUtils;
import study.querydsl.dto.MemberSearchCondition;
import study.querydsl.dto.MemberTeamDto;
import study.querydsl.dto.QMemberTeamDto;
import study.querydsl.entity.Member;

import javax.persistence.EntityManager;
import java.util.List;

import static org.springframework.util.StringUtils.hasText;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.team;

public class MemberRepositoryImpl extends QuerydslRepositorySupport implements MemberRepositoryCustom{

  private final JPAQueryFactory queryFactory;

//  public MemberRepositoryImpl(EntityManager em) {
//    this.queryFactory = new JPAQueryFactory(em);
//  }

  public MemberRepositoryImpl(EntityManager em) {
    super(Member.class);
    this.queryFactory = new JPAQueryFactory(em);
  }

  @Override
  public List<MemberTeamDto> search(MemberSearchCondition condition) {

    return from(member)
            .leftJoin(member.team, team)
            .where(
                    usernameEq(condition.getUsername()),
                    teamNameEq(condition.getTeamName()),
                    ageGoe(condition.getAgeGoe()),
                    ageLoe(condition.getAgeLoe())
            )
            .select(new QMemberTeamDto(
                    member.id.as("memberId"),
                    member.username,
                    member.age,
                    team.id.as("teamId"),
                    team.name.as("teamName")
            ))
            .fetch();

//    return queryFactory
//            .select(new QMemberTeamDto(
//                    member.id.as("memberId"),
//                    member.username,
//                    member.age,
//                    team.id.as("teamId"),
//                    team.name.as("teamName")
//            ))
//            .from(member)
//            .leftJoin(member.team, team)
//            .where(
//                    usernameEq(condition.getUsername()),
//                    teamNameEq(condition.getTeamName()),
//                    ageGoe(condition.getAgeGoe()),
//                    ageLoe(condition.getAgeLoe())
//            )
//            .fetch();
  }

  @Override
  public Page<MemberTeamDto> searchPageSimple(MemberSearchCondition condition, Pageable pageable) {
    QueryResults<MemberTeamDto> results = queryFactory
            .select(new QMemberTeamDto(
                    member.id.as("memberId"),
                    member.username,
                    member.age,
                    team.id.as("teamId"),
                    team.name.as("teamName")
            ))
            .from(member)
            .leftJoin(member.team, team)
            .where(
                    usernameEq(condition.getUsername()),
                    teamNameEq(condition.getTeamName()),
                    ageGoe(condition.getAgeGoe()),
                    ageLoe(condition.getAgeLoe())
            )
            .offset(pageable.getOffset())
            .limit(pageable.getPageSize())
            .fetchResults();

    List<MemberTeamDto> content = results.getResults();
    JPAQuery<Member> countQuery = queryFactory
            .select(member)
            .from(member)
            .where(
                    usernameEq(condition.getUsername()),
                    teamNameEq(condition.getTeamName()),
                    ageGoe(condition.getAgeGoe()),
                    ageLoe(condition.getAgeLoe())
            );

    return PageableExecutionUtils.getPage(content, pageable, countQuery::fetchCount);
  }

  public Page<MemberTeamDto> searchPageSimple2(MemberSearchCondition condition, Pageable pageable) {
    JPQLQuery<MemberTeamDto> jpaQuery = from(member)
            .from(member)
            .leftJoin(member.team, team)
            .where(
                    usernameEq(condition.getUsername()),
                    teamNameEq(condition.getTeamName()),
                    ageGoe(condition.getAgeGoe()),
                    ageLoe(condition.getAgeLoe())
            )
            .select(new QMemberTeamDto(
                    member.id.as("memberId"),
                    member.username,
                    member.age,
                    team.id.as("teamId"),
                    team.name.as("teamName")
            ));

    JPQLQuery<MemberTeamDto> query = getQuerydsl().applyPagination(pageable, jpaQuery);

    List<MemberTeamDto> fetch = query.fetch();

//    List<MemberTeamDto> content = results.getResults();
//            from(member)
//            .select(member)
//            .where(
//                    usernameEq(condition.getUsername()),
//                    teamNameEq(condition.getTeamName()),
//                    ageGoe(condition.getAgeGoe()),
//                    ageLoe(condition.getAgeLoe())
//            );
//    return PageableExecutionUtils.getPage(content, pageable, countQuery::fetchCount);
    return null;
  }

  @Override
  public Page<MemberTeamDto> searchPageComplex(MemberSearchCondition condition, Pageable pageable) {
    List<MemberTeamDto> content = getMemberTeamDtos(condition, pageable);
    long total = getTotal(condition);
    return new PageImpl<>(content, pageable, total);
  }

  private long getTotal(MemberSearchCondition condition) {
    long total = queryFactory
            .select(member)
            .from(member)
//            .leftJoin(member.team, team)
            .where(
                    usernameEq(condition.getUsername()),
                    teamNameEq(condition.getTeamName()),
                    ageGoe(condition.getAgeGoe()),
                    ageLoe(condition.getAgeLoe())
            )
            .fetchCount();
    return total;
  }

  private List<MemberTeamDto> getMemberTeamDtos(MemberSearchCondition condition, Pageable pageable) {
    List<MemberTeamDto> content = queryFactory
            .select(new QMemberTeamDto(
                    member.id.as("memberId"),
                    member.username,
                    member.age,
                    team.id.as("teamId"),
                    team.name.as("teamName")
            ))
            .from(member)
            .leftJoin(member.team, team)
            .where(
                    usernameEq(condition.getUsername()),
                    teamNameEq(condition.getTeamName()),
                    ageGoe(condition.getAgeGoe()),
                    ageLoe(condition.getAgeLoe())
            )
            .offset(pageable.getOffset())
            .limit(pageable.getPageSize())
            .fetch();
    return content;
  }

  private BooleanExpression ageLoe(Integer ageLoe) {
    return ageLoe != null ? member.age.loe(ageLoe) : null;
  }

  private BooleanExpression ageGoe(Integer ageGoe) {
    return ageGoe != null ? member.age.goe(ageGoe) : null;
  }

  private BooleanExpression teamNameEq(String teamName) {
    return hasText(teamName) ? team.name.eq(teamName) : null;
  }

  private BooleanExpression usernameEq(String username) {
    return hasText(username) ? member.username.eq(username) : null;
  }
}
