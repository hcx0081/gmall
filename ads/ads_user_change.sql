-- 建表
drop table if exists ads_user_change;
create external table ads_user_change
(
    dt               string comment '统计日期',
    user_churn_count bigint comment '流失用户数',
    user_back_count  bigint comment '回流用户数'
) comment '用户变动统计'
    row format delimited fields terminated by '\t'
    location '/gmall/warehouse/ads/ads_user_change/';


-- 装载数据
insert overwrite table ads_user_change
select *
from ads_user_change
union
select '2024-05-05',
       user_churn_count,
       user_back_count
from (select '2024-05-05' dt,
             count(*)     user_churn_count
      from dws_user_user_login_td
      where dt = '2024-05-05'
        and login_date_last = date_sub('2024-05-05', 7)) churn -- 流失
         join (select '2024-05-05' dt,
                      count(*)     user_back_count
               from (select user_id,
                            login_date_last
                     from dws_user_user_login_td
                     where dt = '2024-05-05'
                       and login_date_last = '2024-05-05') new
                        join(select user_id,
                                    login_date_last
                             from dws_user_user_login_td
                             where dt = date_sub('2024-05-05', 1)) old on new.user_id = old.user_id
               where datediff(new.login_date_last, old.login_date_last) > 7) back -- 回流
              on churn.dt = back.dt;