package com.gmall.realtime.dws;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.gmall.realtime.common.base.BaseApp;
import com.gmall.realtime.common.bean.UserLogin;
import com.gmall.realtime.common.constant.Constants;
import com.gmall.realtime.common.util.DateFormatUtils;
import com.gmall.realtime.common.util.FlinkSinkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
 * 用户域用户登录各窗口汇总表
 */
public class DwsUserUserLoginWindowApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsUserUserLoginWindowApp().start(10024, 4, "dws-user-user-login-window-app", Constants.TOPIC_DWD_TRAFFIC_PAGE);
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
                    Long ts = jsonObject.getLong("ts");
                    String uid = jsonObject.getJSONObject("common").getString("uid");
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (ts != null && StringUtils.isNotBlank(uid) && (StringUtils.isBlank(lastPageId) || "login".equals(lastPageId))) {
                        // 当前为一次会话的第一条数据
                        out.collect(jsonObject);
                    }
                }
            }
        });
        
        // 添加水位线，保证数据有序
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = jsonObjectStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                 .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                     @Override
                                     public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                         return element.getLong("ts");
                                     }
                                 })
        );
        
        // 按照uid进行分区
        KeyedStream<JSONObject, String> keyedByUidStream = withWatermarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("uid");
            }
        });
        
        // 判断七日回流用户和当日独立用户
        SingleOutputStreamOperator<UserLogin> beanStream = keyedByUidStream.process(new KeyedProcessFunction<String, JSONObject, UserLogin>() {
            ValueState<String> lastLoginDtState;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastLoginDtStateDescriptor = new ValueStateDescriptor<>("last-login-dt", String.class);
                // 无需过期
                // lastLoginDtStateDescriptor.enableTimeToLive(
                //         StateTtlConfig.newBuilder(Time.days(1L))
                //                       .updateTtlOnCreateAndWrite()
                //                       .build()
                // );
                lastLoginDtState = getRuntimeContext().getState(lastLoginDtStateDescriptor);
            }
            
            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, UserLogin>.Context ctx, Collector<UserLogin> out) throws Exception {
                Long ts = value.getLong("ts");
                String curDate = DateFormatUtils.tsToDateString(ts);
                
                String lastLoginDt = lastLoginDtState.value();
                
                // 回流用户数
                Long backCt = 0L;
                // 独立用户数
                Long uuCt = 0L;
                
                // 回流用户
                if (lastLoginDt != null && ts - DateFormatUtils.dateStringToTs(lastLoginDt) > 1000 * 60 * 60 * 24 * 7) {
                    backCt = 1L;
                }
                // 独立用户：新的用户或之前有登录过，但是不是今天
                if (lastLoginDt == null || !lastLoginDt.equals(curDate)) {
                    uuCt = 1L;
                }
                lastLoginDtState.update(curDate);
                
                // 不是独立用户肯定不是回流用户
                if (uuCt == 1) {
                    out.collect(UserLogin.builder()
                                         .curDate(curDate)
                                         .backCt(backCt)
                                         .uuCt(uuCt)
                                         .ts(ts)
                                         .build());
                }
            }
        });
        
        // 开窗
        AllWindowedStream<UserLogin, TimeWindow> windowedStream =
                beanStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));
        
        // 聚合
        SingleOutputStreamOperator<UserLogin> reducedStream = windowedStream.reduce(new ReduceFunction<UserLogin>() {
            @Override
            public UserLogin reduce(UserLogin value1, UserLogin value2) throws Exception {
                value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                return value1;
            }
        }, new ProcessAllWindowFunction<UserLogin, UserLogin, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<UserLogin, UserLogin, TimeWindow>.Context context, Iterable<UserLogin> elements, Collector<UserLogin> out) throws Exception {
                TimeWindow window = context.window();
                
                String stt = DateFormatUtils.tsToDateTimeString(window.getStart());
                String edt = DateFormatUtils.tsToDateTimeString(window.getEnd());
                String curDate = DateFormatUtils.tsToDateString(System.currentTimeMillis());
                for (UserLogin userLogin : elements) {
                    userLogin.setStt(stt);
                    userLogin.setEdt(edt);
                    userLogin.setCurDate(curDate);
                    out.collect(userLogin);
                }
            }
        });
        
        SingleOutputStreamOperator<String> mapStream = reducedStream.map(JSON::toJSONString);
        mapStream.sinkTo(FlinkSinkUtils.getDorisSink(Constants.DORIS_TABLE_DWS_USER_USER_LOGIN_WINDOW));
    }
}
