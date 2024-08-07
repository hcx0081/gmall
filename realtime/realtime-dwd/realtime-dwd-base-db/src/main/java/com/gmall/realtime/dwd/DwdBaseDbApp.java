package com.gmall.realtime.dwd;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONReader;
import com.gmall.realtime.common.base.BaseApp;
import com.gmall.realtime.common.bean.TableProcessDwd;
import com.gmall.realtime.common.constant.Constants;
import com.gmall.realtime.common.util.FlinkSinkUtils;
import com.gmall.realtime.common.util.FlinkSourceUtils;
import com.gmall.realtime.dwd.function.DwdBroadcastProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DwdBaseDbApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwdBaseDbApp().start(10017, 4, "dwd-base-db-app", Constants.TOPIC_DB);
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> source) {
        /* 1. 从Kafka中ETL数据 */
        SingleOutputStreamOperator<JSONObject> jsonObjectStream = etlData(source);
        
        /* 2. 使用Flink-CDC监控配置表数据 */
        DataStreamSource<String> mySqlSource =
                env.fromSource(
                           FlinkSourceUtils.getMySqlSource(Constants.GMALL_CONFIG_DATABASE, Constants.GMALL_CONFIG_DWD_TABLE),
                           WatermarkStrategy.noWatermarks(),
                           "cdc-source"
                   )
                   // 注意需要设置并行度为1
                   .setParallelism(1);
        
        /* 2. 转换*/
        SingleOutputStreamOperator<TableProcessDwd> tableProcessDwdStream =
                mySqlSource.flatMap(new RichFlatMapFunction<String, TableProcessDwd>() {
                               @Override
                               public void flatMap(String value, Collector<TableProcessDwd> out) throws Exception {
                                   boolean isJsonObject = JSON.isValidObject(value);
                                   if (isJsonObject) {
                                       JSONObject jsonObject = JSON.parseObject(value);
                                       String op = jsonObject.getString("op");
                                       TableProcessDwd tableProcessDwd = new TableProcessDwd();
                                       if ("d".equals(op)) {
                                           tableProcessDwd = jsonObject.getObject("before", TableProcessDwd.class, JSONReader.Feature.SupportSmartMatch);
                                       } else {
                                           tableProcessDwd = jsonObject.getObject("after", TableProcessDwd.class, JSONReader.Feature.SupportSmartMatch);
                                       }
                                       tableProcessDwd.setOp(op);
                                       out.collect(tableProcessDwd);
                                   }
                               }
                           })
                           // 注意需要设置并行度为1
                           .setParallelism(1);
        
        /* 4. 创建广播流 */
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>(
                "broadcast-state",
                String.class,
                TableProcessDwd.class
        );
        BroadcastStream<TableProcessDwd> broadcastStream = tableProcessDwdStream.broadcast(mapStateDescriptor);
        
        /* 5. 连接主流和广播流 */
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectedStream = jsonObjectStream.connect(broadcastStream);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream =
                connectedStream.process(new DwdBroadcastProcessFunction(mapStateDescriptor))
                               // 注意需要设置并行度为1
                               .setParallelism(1);
        
        /* 6. 过滤不需要的字段 */
        SingleOutputStreamOperator<JSONObject> dataStream = processStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, JSONObject>() {
            @Override
            public JSONObject map(Tuple2<JSONObject, TableProcessDwd> value) throws Exception {
                JSONObject jsonObject = value.f0;
                TableProcessDwd tableProcessDwd = value.f1;
                JSONObject data = jsonObject.getJSONObject("data");
                data.keySet().removeIf(key -> !tableProcessDwd.getSinkColumns().contains(key));
                data.put("sink_table", tableProcessDwd.getSinkTable());
                return data;
            }
        });
        
        dataStream.sinkTo(FlinkSinkUtils.getKafkaSink());
    }
    
    private SingleOutputStreamOperator<JSONObject> etlData(DataStreamSource<String> source) {
        SingleOutputStreamOperator<JSONObject> jsonObjectStream = source.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                boolean isJsonObject = JSON.isValidObject(value);
                if (isJsonObject) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                }
            }
        });
        return jsonObjectStream;
    }
}
