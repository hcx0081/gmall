package com.gmall.realtime.common.util;

import com.gmall.realtime.common.constant.Constants;

/**
 * Flink SQL工具类
 */
public class FlinkSQLUtils {
    public static String withSQLFromKafka(String topic, String groupId) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constants.KAFKA_BOOTSTRAP_SERVERS + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }
    
    public static String withSQLToKafka(String topic) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constants.KAFKA_BOOTSTRAP_SERVERS + "',\n" +
                "  'format' = 'json'\n" +
                ")";
    }
    
    public static String withSQLToDoris(String table) {
        return "WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '" + Constants.DORIS_FE_NODES + "',\n" +
                "  'table.identifier' = '" + Constants.DORIS_DATABASE + "." + table + "',\n" +
                "  'username' = '" + Constants.DORIS_USERNAME + "',\n" +
                "  'password' = '" + Constants.DORIS_PASSWORD + "',\n" +
                "  'sink.properties.format' = 'json',\n" +
                "  'sink.properties.read_json_by_line' = 'true',\n" +
                "  'sink.label-prefix' = 'doris_label_" + System.currentTimeMillis() + "'\n" +
                ")";
    }
    
    public static String withSQLToUpsertKafka(String topic) {
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constants.KAFKA_BOOTSTRAP_SERVERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }
    
    public static String createTopicDbSourceFromKafka() {
        return "CREATE TABLE topic_db_source\n" +
                "(\n" +
                "    `database` STRING,\n" +
                "    `table`    STRING,\n" +
                "    `type`     STRING,\n" +
                "    `ts`       BIGINT,\n" +
                "    `data`     MAP<STRING, STRING>,\n" +
                "    `old`      MAP<STRING, STRING>,\n" +
                "    `proc_time` as PROCTIME(),\n" +
                
                "    `event_time` as TO_TIMESTAMP_LTZ(`ts`, 0),\n" +
                "    WATERMARK FOR event_time AS event_time - INTERVAL '4' SECOND\n" +
                ")" + withSQLFromKafka(Constants.TOPIC_DB, Constants.TOPIC_DB);
    }
    
    public static String createBaseDicSourceFromHBase() {
        return "CREATE TABLE base_dic_source\n" +
                "(\n" +
                "    rowkey STRING,\n" +
                "    info ROW <dic_name STRING>,\n" +
                "    PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'gmall:dim_base_dic',\n" +
                " 'zookeeper.quorum' = '" + Constants.HBASE_ZOOKEEPER_QUORUM + ":2181'\n" +
                ")";
    }
}
