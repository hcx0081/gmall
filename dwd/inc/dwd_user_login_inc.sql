-- 建表
drop table if exists dwd_user_login_inc;
create external table dwd_user_login_inc
(
    user_id        string comment '用户id',
    date_id        string comment '日期id',
    login_time     string comment '登录时间',
    channel        string comment '应用下载渠道',
    province_id    string comment '省份id',
    version_code   string comment '应用版本',
    mid_id         string comment '设备id',
    brand          string comment '设备品牌',
    model          string comment '设备型号',
    operate_system string comment '设备操作系统'
) comment '用户域用户登录事务事实表'
    partitioned by (dt string)
    stored as orc
    location '/gmall/warehouse/dwd/dwd_user_login_inc/'
    tblproperties ("orc.compress" = "snappy");


-- 装载数据
insert overwrite table dwd_user_login_inc partition (dt = '2024-05-05')
select user_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') login_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system
from (select ts,
             common.uid                                                  user_id,
             common.ch                                                   channel,
             common.ar                                                   province_id,
             common.vc                                                   version_code,
             common.mid                                                  mid_id,
             common.ba                                                   brand,
             common.md                                                   model,
             common.os                                                   operate_system,
             row_number() over (partition by common.sid order by ts asc) rn
      from ods_log_inc
      where dt = '2024-05-05'
        and page.page_id is not null
        and common.uid is not null) t
where rn = 1;