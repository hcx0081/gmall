package com.gmall.realtime.dim;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.gmall.realtime.common.base.BaseApp;
import com.gmall.realtime.common.constant.Constants;
import com.gmall.realtime.common.util.FlinkSourceUtils;
import com.gmall.realtime.common.util.HBaseUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DimApp().start(10001, 4, "dim_app", Constants.TOPIC_DB);
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> source) {
        /* 1. ETL数据 */
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etlData(source);
        
        
        /* 2. 使用Flink-CDC监控配置表数据 */
        DataStreamSource<String> mySqlSource = env.fromSource(
                        FlinkSourceUtils.getMySqlSource(Constants.CONFIG_DATABASE, Constants.CONFIG_TABLE),
                        WatermarkStrategy.noWatermarks(),
                        "cdc-source"
                )
                // 注意需要设置并行度为1
                .setParallelism(1);
        mySqlSource.flatMap(new RichFlatMapFunction<String, JSONObject>() {
            Connection connection;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                connection = HBaseUtils.getConnection();
                
            }
            
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                boolean isJsonObj = JSON.isValidObject(value);
                if (isJsonObj) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    
                    
                    HBaseUtils.createNamespace(Constants.HBASE_NAMESPACE);
                }
                
            }
            
            @Override
            public void close() throws Exception {
                HBaseUtils.closeConnection();
            }
        });
    }
    
    private SingleOutputStreamOperator<JSONObject> etlData(DataStreamSource<String> source) {
        // return source.filter(new FilterFunction<String>() {
        //     @Override
        //     public boolean filter(String value) throws Exception {
        //         boolean isJsonObj = JSON.isValidObject(value);
        //         if (isJsonObj) {
        //             JSONObject jsonObject = JSON.parseObject(value);
        //             String database = jsonObject.getString("database");
        //             String type = jsonObject.getString("type");
        //             JSONObject data = jsonObject.getJSONObject("data");
        //             if ("gmall".equals(database) &&
        //                     !"bootstrap-start".equals(type) && !"bootstrap-end".equals(type) &&
        //                     MapUtils.isNotEmpty(data)) {
        //                 return true;
        //             } else {
        //                 return false;
        //             }
        //         }
        //         return false;
        //     }
        // }).map(new MapFunction<String, JSONObject>() {
        //     @Override
        //     public JSONObject map(String value) throws Exception {
        //         return JSON.parseObject(value);
        //     }
        // });
        
        return source.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                boolean isJsonObj = JSON.isValidObject(value);
                if (isJsonObj) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String database = jsonObject.getString("database");
                    String type = jsonObject.getString("type");
                    JSONObject data = jsonObject.getJSONObject("data");
                    if ("gmall".equals(database) &&
                            !"bootstrap-start".equals(type) && !"bootstrap-end".equals(type) &&
                            MapUtils.isNotEmpty(data)) {
                        out.collect(JSON.parseObject(value));
                    }
                }
            }
        });
    }
}
