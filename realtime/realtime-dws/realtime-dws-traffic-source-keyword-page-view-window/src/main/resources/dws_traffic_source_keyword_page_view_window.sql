create database if not exists gmall;

drop table if exists gmall.dws_traffic_source_keyword_page_view_window;
create table if not exists gmall.dws_traffic_source_keyword_page_view_window
(
    `stt`           DATETIME COMMENT '窗口起始时间',
    `edt`           DATETIME COMMENT '窗口结束时间',
    `cur_date`      DATE COMMENT '当天日期',
    `keyword`       VARCHAR(128) COMMENT '搜索关键词',
    `keyword_count` BIGINT REPLACE COMMENT '搜索关键词出现次数'
) engine = olap aggregate key (`stt`,`edt`,`cur_date`,`keyword`)
partition by range(`cur_date`)()
distributed by hash(`stt`) buckets 10
properties (
"replication_num" = "1",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "p",
"dynamic_partition.buckets" = "10"
);