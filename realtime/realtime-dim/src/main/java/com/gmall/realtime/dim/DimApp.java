package com.gmall.realtime.dim;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONReader;
import com.gmall.realtime.common.base.BaseApp;
import com.gmall.realtime.common.bean.TableProcessDim;
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

public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DimApp().start(10001, 4, "dim_app", Constants.TOPIC_DB);
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> source) {
        /* 1. ETL数据 */
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etlData(source);
        
        /* 2. 使用Flink-CDC监控配置表数据 */
        DataStreamSource<String> mySqlSource =
                env.fromSource(
                           FlinkSourceUtils.getMySqlSource(Constants.CONFIG_DATABASE, Constants.CONFIG_TABLE),
                           WatermarkStrategy.noWatermarks(),
                           "cdc-source"
                   )
                   // 注意需要设置并行度为1
                   .setParallelism(1);
        SingleOutputStreamOperator<TableProcessDim> createHBaseTableStream = mySqlSource.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {
            /* 注意：需要先在HBase中创建gmall命名空间：create_namespace 'gmall'！ */
            
            @Override
            public void open(Configuration parameters) throws Exception {
                HBaseUtils.getConnection();
            }
            
            @Override
            public void flatMap(String value, Collector<TableProcessDim> out) throws Exception {
                boolean isJsonObj = JSON.isValidObject(value);
                if (isJsonObj) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String op = jsonObject.getString("op");
                    TableProcessDim tableProcessDim = new TableProcessDim();
                    if ("d".equals(op)) {
                        tableProcessDim = jsonObject.getObject("before", TableProcessDim.class, JSONReader.Feature.SupportSmartMatch);
                        deleteTable(tableProcessDim);
                    }
                    if ("c".equals(op) || "r".equals(op)) {
                        tableProcessDim = jsonObject.getObject("after", TableProcessDim.class, JSONReader.Feature.SupportSmartMatch);
                        createTable(tableProcessDim);
                    }
                    if ("u".equals(op)) {
                        tableProcessDim = jsonObject.getObject("after", TableProcessDim.class, JSONReader.Feature.SupportSmartMatch);
                        deleteTable(tableProcessDim);
                        createTable(tableProcessDim);
                    }
                    tableProcessDim.setOp(op);
                    out.collect(tableProcessDim);
                }
            }
            
            private void createTable(TableProcessDim tableProcessDim) {
                HBaseUtils.createTable(Constants.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), tableProcessDim.getSinkFamily());
            }
            
            private void deleteTable(TableProcessDim tableProcessDim) {
                HBaseUtils.deleteTable(Constants.HBASE_NAMESPACE, tableProcessDim.getSinkTable());
            }
            
            @Override
            public void close() throws Exception {
                HBaseUtils.closeConnection();
            }
        });
        
        createHBaseTableStream.print();
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
