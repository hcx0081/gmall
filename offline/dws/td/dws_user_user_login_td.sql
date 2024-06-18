-- 建表
drop table if exists dws_user_user_login_td;
create external table dws_user_user_login_td
(
    user_id          string comment '用户id',
    login_date_last  string comment '历史至今末次登录日期',
    login_date_first string comment '历史至今首次登录日期',
    login_count_td   bigint comment '历史至今累计登录次数'
) comment '用户域用户粒度登录历史至今汇总表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dws/dws_user_user_login_td'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据（首日）
insert overwrite table dws_user_user_login_td partition (dt = '2024-05-05')
select user_id,
       max(dt)  login_date_last,
       min(dt)  login_date_first,
       count(*) login_count_td
from dwd_user_login_inc
group by user_id;


-- 装载数据（每日）
insert overwrite table dws_user_user_login_td partition (dt = '2024-05-06')
select user_id,
       max(login_date_last),
       min(login_date_first),
       sum(login_count_td)
from (select user_id,
             login_date_last,
             login_date_first,
             login_count_td
      from dws_user_user_login_td
      where dt = date_sub('2024-05-06', 1)
      union all
      select user_id,
             '2024-05-06',
             '2024-05-06',
             count(*)
      from dwd_user_login_inc
      where dt = '2024-05-06'
      group by user_id) t
group by user_id;


select id           user_id,
       '2024-05-05' login_date_last,
       '2024-05-05' login_date_first,
       1            login_count_td
from dim_user_zip
where dt = '9999-12-31';