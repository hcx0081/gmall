-- 建表
drop table if exists ads_user_stats;
create external table ads_user_stats
(
    dt                string comment '统计日期',
    recent_days       bigint comment '最近n日,1:最近1日,7:最近7日,30:最近30日',
    new_user_count    bigint comment '新增用户数',
    active_user_count bigint comment '活跃用户数'
) comment '用户新增活跃统计'
    row format delimited fields terminated by '\t'
    location '/gmall/warehouse/ads/ads_user_stats/';


-- 装载数据
insert overwrite table ads_user_stats
select *
from ads_user_stats
union
select *
from (select '2024-05-05',
             1,
             sum(if(login_date_first = date_sub('2024-05-05', 1), 1, 0)) new_user_count,
             count(*)                                                    active_user_count
      from dws_user_user_login_td
      where dt = '2024-05-05'
        and login_date_last = date_sub('2024-05-05', 1)
      union all
      select '2024-05-05',
             7,
             sum(if(login_date_first >= date_sub('2024-05-05', 7), 1, 0)) new_user_count,
             count(*)                                                     active_user_count
      from dws_user_user_login_td
      where dt = '2024-05-05'
        and login_date_last >= date_sub('2024-05-05', 7)
        and login_date_last < '2024-05-05'
      union all
      select '2024-05-05',
             30,
             sum(if(login_date_first >= date_sub('2024-05-05', 30), 1, 0)) new_user_count,
             count(*)                                                      active_user_count
      from dws_user_user_login_td
      where dt = '2024-05-05'
        and login_date_last >= date_sub('2024-05-05', 30)
        and login_date_last < '2024-05-05') t;


-- 装载数据（优化）
insert overwrite table ads_user_stats
select *
from ads_user_stats
union
select '2024-05-05',
       recent_days,
       sum(if(login_date_first >= date_sub('2024-05-05', recent_days), 1, 0)) new_user_count,
       count(*)                                                               active_user_count
from dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt = '2024-05-05'
  and login_date_last >= date_sub('2024-05-05', recent_days)
  and login_date_last < '2024-05-05'
group by recent_days;