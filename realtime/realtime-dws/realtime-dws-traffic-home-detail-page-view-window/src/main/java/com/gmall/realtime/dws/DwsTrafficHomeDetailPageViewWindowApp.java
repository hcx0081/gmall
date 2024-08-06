package com.gmall.realtime.dws;

import com.gmall.realtime.common.base.BaseSQLApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 流量域搜索关键词粒度页面浏览各窗口汇总表
 */
public class DwsTrafficHomeDetailPageViewWindowApp extends BaseSQLApp {
    public static void main(String[] args) throws Exception {
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
    }
}
