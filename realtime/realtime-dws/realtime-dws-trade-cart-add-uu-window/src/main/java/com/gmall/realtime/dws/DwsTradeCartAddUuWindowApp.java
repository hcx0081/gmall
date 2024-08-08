package com.gmall.realtime.dws;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.gmall.realtime.common.base.BaseApp;
import com.gmall.realtime.common.bean.CartAddUu;
import com.gmall.realtime.common.constant.Constants;
import com.gmall.realtime.common.util.DateFormatUtils;
import com.gmall.realtime.common.util.FlinkSinkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 交易域加购各窗口汇总表
 */
public class DwsTradeCartAddUuWindowApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeCartAddUuWindowApp().start(10026, 4, "dws-trade-cart-add-uu-window-app", Constants.TOPIC_DWD_TRADE_CART_ADD);
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
                    String userId = jsonObject.getString("user_id");
                    Long ts = jsonObject.getLong("ts");
                    if (ts != null && StringUtils.isNotBlank(userId)) {
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
        
        // 根据user_id进行分区
        KeyedStream<JSONObject, String> keyedByUserIdStream = withWatermarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("user_id");
            }
        });
        
        // 判断独立用户
        SingleOutputStreamOperator<CartAddUu> beanStream = keyedByUserIdStream.process(new KeyedProcessFunction<String, JSONObject, CartAddUu>() {
            ValueState<String> lastLoginDtState;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastLoginDtStateDescriptor = new ValueStateDescriptor<>("last-login-dt", String.class);
                lastLoginDtStateDescriptor.enableTimeToLive(
                        StateTtlConfig.newBuilder(Time.days(1L))
                                      .updateTtlOnCreateAndWrite()
                                      .build()
                );
                lastLoginDtState = getRuntimeContext().getState(lastLoginDtStateDescriptor);
                
            }
            
            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, CartAddUu>.Context ctx, Collector<CartAddUu> out) throws Exception {
                Long ts = value.getLong("ts");
                String curDate = DateFormatUtils.tsToDateString(ts);
                String lastLoginDt = lastLoginDtState.value();
                Long cartAddUuCt = 0L;
                if (lastLoginDt == null || !lastLoginDt.equals(curDate)) {
                    cartAddUuCt = 1L;
                    lastLoginDtState.update(curDate);
                }
                
                if (cartAddUuCt == 1L) {
                    out.collect(
                            CartAddUu.builder()
                                     .curDate(curDate)
                                     .cartAddUuCt(cartAddUuCt)
                                     .build()
                    );
                }
            }
        });
        
        // 开窗
        AllWindowedStream<CartAddUu, TimeWindow> windowedStream =
                beanStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));
        
        // 聚合
        SingleOutputStreamOperator<CartAddUu> reducedStream = windowedStream.reduce(new ReduceFunction<CartAddUu>() {
            @Override
            public CartAddUu reduce(CartAddUu value1, CartAddUu value2) throws Exception {
                value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                return value1;
            }
        }, new ProcessAllWindowFunction<CartAddUu, CartAddUu, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<CartAddUu, CartAddUu, TimeWindow>.Context context, Iterable<CartAddUu> elements, Collector<CartAddUu> out) throws Exception {
                TimeWindow window = context.window();
                
                String stt = DateFormatUtils.tsToDateTimeString(window.getStart());
                String edt = DateFormatUtils.tsToDateTimeString(window.getEnd());
                String curDate = DateFormatUtils.tsToDateString(System.currentTimeMillis());
                for (CartAddUu cartAddUu : elements) {
                    cartAddUu.setStt(stt);
                    cartAddUu.setEdt(edt);
                    cartAddUu.setCurDate(curDate);
                    out.collect(cartAddUu);
                }
            }
        });
        
        SingleOutputStreamOperator<String> mapStream = reducedStream.map(JSON::toJSONString);
        mapStream.sinkTo(FlinkSinkUtils.getDorisSink(Constants.DORIS_TABLE_DWS_TRADE_CART_ADD_UU_WINDOW));
    }
}
