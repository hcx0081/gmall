package com.gmall.realtime.dwd;

import com.gmall.realtime.common.base.BaseApp;
import com.gmall.realtime.common.constant.Constants;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwdBaseDbApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwdBaseDbApp().start(10017, 4, "dwd-base-db-app", Constants.TOPIC_LOG);
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> source) {
    
    }
}