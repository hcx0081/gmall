package com.gmall.realtime.dws;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.gmall.realtime.common.base.BaseApp;
import com.gmall.realtime.common.bean.TradeProvinceOrder;
import com.gmall.realtime.common.constant.Constants;
import com.gmall.realtime.common.util.DateFormatUtils;
import com.gmall.realtime.common.util.FlinkSinkUtils;
import com.gmall.realtime.common.util.HBaseUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashSet;

/**
 * 交易域省份粒度下单各窗口汇总表
 */
public class DwsTradeProvinceOrderWindowApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeProvinceOrderWindowApp().start(10030, 4, "dws-trade-province-order-window-app", Constants.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> source) {
        // 过滤脏数据
        SingleOutputStreamOperator<JSONObject> jsonObjectStream = source.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                boolean isJsonObject = JSON.isValidObject(value);
                if (isJsonObject) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String id = jsonObject.getString("id");
                    String orderId = jsonObject.getString("order_id");
                    String provinceId = jsonObject.getString("province_id");
                    Long ts = jsonObject.getLong("ts");
                    if (StringUtils.isNotBlank(id) && StringUtils.isNotBlank(orderId) && StringUtils.isNotBlank(provinceId) && ts != null) {
                        jsonObject.put("ts", ts * 1000);
                        out.collect(jsonObject);
                    }
                }
            }
        });
        
        // 添加水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = jsonObjectStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                 .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                     @Override
                                     public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                         return element.getLong("ts");
                                     }
                                 })
        );
        
        // 按照id进行分区
        KeyedStream<JSONObject, String> keyedByIdStream = withWatermarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("id");
            }
        });
        
        // 修正度量值，转换数据结构
        SingleOutputStreamOperator<TradeProvinceOrder> beanStream = keyedByIdStream.map(new RichMapFunction<JSONObject, TradeProvinceOrder>() {
            ValueState<BigDecimal> lastTotalAmountState;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<BigDecimal> lastTotalAmountStateDescriptor = new ValueStateDescriptor<>("last-total-amount", BigDecimal.class);
                lastTotalAmountStateDescriptor.enableTimeToLive(
                        StateTtlConfig.newBuilder(Time.seconds(30L))
                                      .updateTtlOnCreateAndWrite()
                                      .build()
                );
                lastTotalAmountState = getRuntimeContext().getState(lastTotalAmountStateDescriptor);
            }
            
            @Override
            public TradeProvinceOrder map(JSONObject value) throws Exception {
                HashSet<String> hashSet = new HashSet<>();
                hashSet.add(value.getString("order_id"));
                
                BigDecimal lastTotalAmount = lastTotalAmountState.value();
                lastTotalAmount = lastTotalAmount == null ? BigDecimal.valueOf(0) : lastTotalAmount;
                BigDecimal splitTotalAmount = value.getBigDecimal("split_total_amount");
                lastTotalAmountState.update(splitTotalAmount);
                
                return TradeProvinceOrder.builder()
                                         .provinceId(value.getString("province_id"))
                                         .orderAmount(splitTotalAmount.subtract(lastTotalAmount))
                                         .orderDetailId(value.getString("id"))
                                         .ts(value.getLong("ts"))
                                         .build();
            }
        });
        
        // 按照provinceId进行分区
        KeyedStream<TradeProvinceOrder, String> keyedByProvinceIdStream = beanStream.keyBy(new KeySelector<TradeProvinceOrder, String>() {
            @Override
            public String getKey(TradeProvinceOrder value) throws Exception {
                return value.getProvinceId();
            }
        });
        
        // 开窗
        WindowedStream<TradeProvinceOrder, String, TimeWindow> windowedStream =
                keyedByProvinceIdStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));
        
        // 聚合
        SingleOutputStreamOperator<TradeProvinceOrder> reducedStream = windowedStream.reduce(new ReduceFunction<TradeProvinceOrder>() {
            @Override
            public TradeProvinceOrder reduce(TradeProvinceOrder value1, TradeProvinceOrder value2) throws Exception {
                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                return value1;
            }
        }, new ProcessWindowFunction<TradeProvinceOrder, TradeProvinceOrder, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<TradeProvinceOrder, TradeProvinceOrder, String, TimeWindow>.Context context, Iterable<TradeProvinceOrder> elements, Collector<TradeProvinceOrder> out) throws Exception {
                TimeWindow window = context.window();
                
                String stt = DateFormatUtils.tsToDateTimeString(window.getStart());
                String edt = DateFormatUtils.tsToDateTimeString(window.getEnd());
                String curDate = DateFormatUtils.tsToDateString(System.currentTimeMillis());
                for (TradeProvinceOrder tradeProvinceOrder : elements) {
                    tradeProvinceOrder.setStt(stt);
                    tradeProvinceOrder.setEdt(edt);
                    tradeProvinceOrder.setCurDate(curDate);
                    tradeProvinceOrder.setOrderCount((long) tradeProvinceOrder.getOrderIdSet().size());
                    out.collect(tradeProvinceOrder);
                }
            }
        });
        
        // 关联维度信息
        SingleOutputStreamOperator<TradeProvinceOrder> fullDimStream = reducedStream.map(new RichMapFunction<TradeProvinceOrder, TradeProvinceOrder>() {
            private Connection connection;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                connection = HBaseUtils.createConnection();
            }
            
            @Override
            public void close() throws Exception {
                HBaseUtils.closeConnection(connection);
            }
            
            @Override
            public TradeProvinceOrder map(TradeProvinceOrder value) throws Exception {
                
                
                return value;
            }
        });
        
        SingleOutputStreamOperator<String> mapStream = fullDimStream.map(JSON::toJSONString);
        mapStream.sinkTo(FlinkSinkUtils.getDorisSink(Constants.DORIS_TABLE_DWS_TRADE_PROVINCE_ORDER_WINDOW));
    }
}
