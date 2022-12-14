package com.atguigu.gmall.mapper;

import com.atguigu.gmall.bean.KeywordStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * Author: Felix
 * Date: 2021/3/1
 * Desc: 关键词统计Mapper
 */
public interface KeywordStatsMapper {
    //用到了Mybatis做简化
    @Select("select keyword," +
        "sum(keyword_stats_0820.ct * " +
        "multiIf(source='SEARCH',10,source='ORDER',3,source='CART',2,source='CLICK',1,0)) ct" +
        " from keyword_stats_0820 where toYYYYMMDD(stt)=#{date} group by keyword " +
        "order by sum(keyword_stats_0820.ct) desc limit #{limit} ")
    public List<KeywordStats> selectKeywordStats(@Param("date") int date, @Param("limit") int limit);
}

