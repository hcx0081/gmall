create database if not exists gmall;

drop table if exists gmall.dws_user_user_login_window;
create table if not exists gmall.dws_user_user_login_window
(
    `stt`      DATETIME COMMENT '窗口起始时间',
    `edt`      DATETIME COMMENT '窗口结束时间',
    `cur_date` DATE COMMENT '当天日期',
    `back_ct`  BIGINT REPLACE COMMENT '回流用户数',
    `uu_ct`    BIGINT REPLACE COMMENT '独立用户数'
) engine = olap aggregate key (`stt`,`edt`,`cur_date`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "1",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "par",
"dynamic_partition.buckets" = "10"
);