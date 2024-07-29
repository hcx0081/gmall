package com.gmall.realtime.dwd;

import com.gmall.realtime.common.base.BaseSQLApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderCancelDetailApp extends BaseSQLApp {
    public static void main(String[] args) throws Exception {
        new DwdTradeOrderCancelDetailApp().start(10015, 4, "dwd-trade-order-cancel-detail-app");
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        /* 必须设置超时时长！！！ */
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(60 * 30 + 5));
    }
}
