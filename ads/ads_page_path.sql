-- 建表
drop table if exists ads_page_path;
create external table ads_page_path
(
    dt         string comment '统计日期',
    source     string comment '跳转起始页面id',
    target     string comment '跳转终到页面id',
    path_count bigint comment '跳转次数'
) comment '页面浏览路径分析'
    row format delimited fields terminated by '\t'
    location '/gmall/warehouse/ads/ads_page_path/';


-- 装载数据
insert overwrite table ads_page_path
select *
from ads_page_path
union
select '2024-05-05',
       source,
       target,
       count(*)
from (select concat('step-', rn, ':', source)     source,
             concat('step-', rn + 1, ':', target) target
      from (select page_id                                                                   source,
                   lead(page_id, 1, 'out') over (partition by session_id order by view_time) target,
                   row_number() over (partition by session_id order by view_time)            rn
            from dwd_traffic_page_view_inc
            where dt = '2024-05-05') tmp) t
group by source, target;
