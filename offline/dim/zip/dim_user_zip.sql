-- 建表
drop table if exists dim_user_zip;
create external table dim_user_zip
(
    `id`           string comment '用户id',
    `name`         string comment '用户姓名',
    `phone_num`    string comment '手机号码',
    `email`        string comment '邮箱',
    `user_level`   string comment '用户等级',
    `birthday`     string comment '生日',
    `gender`       string comment '性别',
    `create_time`  string comment '创建时间',
    `operate_time` string comment '操作时间',
    `start_date`   string comment '开始日期',
    `end_date`     string comment '结束日期'
) comment '用户维度表'
    partitioned by (`dt` string)
    stored as orc
    location '/gmall/warehouse/dim/dim_user_zip/'
    tblproperties ('orc.compress' = 'snappy');


-- 装载数据（首日）（注意此处的分区策略）
insert overwrite table dim_user_zip partition (dt = '9999-12-31')
select data.`id`,
       data.`name`,
       data.`phone_num`,
       data.`email`,
       data.`user_level`,
       data.`birthday`,
       data.`gender`,
       data.`create_time`,
       data.`operate_time`,
       '2024-05-05',
       '9999-12-31'
from ods_user_info_inc
where dt = '2024-05-05'
  and type = 'bootstrap-insert';


-- 装载数据（每日）
--  增加的用户信息
--      type: insert
--      start: 当天（2024-05-06）
--      end: 9999-12-31
--      partition: 9999-12-31
--  修改的用户信息
--      修改之前:
--          type: update
--          start: start
--          end: 9999-12-31
--          partition: 9999-12-31
--      修改之后:
--          type: update
--          start: 当天（2024-05-06）
--          end: 9999-12-31 -> 2024-05-05
--          partition: 9999-12-31 -> 2024-05-05
insert overwrite table dim_user_zip partition (dt)
select `id`,
       `name`,
       `phone_num`,
       `email`,
       `user_level`,
       `birthday`,
       `gender`,
       `create_time`,
       `operate_time`,
       `start_date`,
       `if`(rn == 2, date_sub('2024-05-06', 1), '9999-12-31'),
       `if`(rn == 2, date_sub('2024-05-06', 1), '9999-12-31')
from (select *,
             row_number() over (partition by id order by start_date desc ) rn
      from (select `id`,
                   `name`,
                   `phone_num`,
                   `email`,
                   `user_level`,
                   `birthday`,
                   `gender`,
                   `create_time`,
                   `operate_time`,
                   `start_date`,
                   `end_date`
            from dim_user_zip
            where dt = '9999-12-31'
            union all
            select data.id,
                   data.name,
                   data.phone_num,
                   data.email,
                   data.user_level,
                   data.birthday,
                   data.gender,
                   data.create_time,
                   data.operate_time,
                   '2024-05-06',
                   '9999-12-31'
            from ods_user_info_inc
            where dt = '2024-05-06'
              and type in ('insert', 'update')) t) t1;


-- 解决多次修改出现的问题（insert不会多次，update可能多次）
insert overwrite table dim_user_zip partition (dt)
select `id`,
       `name`,
       `phone_num`,
       `email`,
       `user_level`,
       `birthday`,
       `gender`,
       `create_time`,
       `operate_time`,
       `start_date`,
       `if`(rn == 2, date_sub('2024-05-06', 1), '9999-12-31'),
       `if`(rn == 2, date_sub('2024-05-06', 1), '9999-12-31')
from (select *,
             row_number() over (partition by id order by start_date desc ) rn
      from (select `id`,
                   `name`,
                   `phone_num`,
                   `email`,
                   `user_level`,
                   `birthday`,
                   `gender`,
                   `create_time`,
                   `operate_time`,
                   `start_date`,
                   `end_date`
            from dim_user_zip
            where dt = '9999-12-31'
            union all
            select id,
                   name,
                   phone_num,
                   email,
                   user_level,
                   birthday,
                   gender,
                   create_time,
                   operate_time,
                   '2024-05-06',
                   '9999-12-31'
            from (select data.id,
                         data.name,
                         data.phone_num,
                         data.email,
                         data.user_level,
                         data.birthday,
                         data.gender,
                         data.create_time,
                         data.operate_time,
                         row_number() over (partition by data.id order by ts desc ) rn
                  from ods_user_info_inc
                  where dt = '2024-05-06'
                    and type in ('insert', 'update')) t1
            where rn = 1) t2) t3;
