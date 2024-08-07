package com.gmall.realtime.dws;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.gmall.realtime.common.base.BaseApp;
import com.gmall.realtime.common.bean.UserRegister;
import com.gmall.realtime.common.constant.Constants;
import com.gmall.realtime.common.util.DateFormatUtils;
import com.gmall.realtime.common.util.FlinkSinkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 用户域用户注册各窗口汇总表
 */
public class DwsUserUserRegisterWindowApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        // 注意：此处并行度设置为1
        new DwsUserUserRegisterWindowApp().start(10025, 1, "dws-user-user-register-window-app", Constants.TOPIC_DWD_USER_REGISTER);
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> source) {
        // 过滤脏数据，并且转换为Bean
        SingleOutputStreamOperator<UserRegister> beanStream = source.flatMap(new FlatMapFunction<String, UserRegister>() {
            @Override
            public void flatMap(String value, Collector<UserRegister> out) throws Exception {
                boolean isJsonObject = JSON.isValidObject(value);
                if (isJsonObject) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    Long id = jsonObject.getLong("id");
                    String createTime = jsonObject.getString("create_time");
                    if (id != null && StringUtils.isNotBlank(createTime)) {
                        out.collect(UserRegister.builder()
                                                .registerCt(1L)
                                                .createTime(createTime)
                                                .build());
                    }
                }
            }
        });
        
        // 添加水位线
        SingleOutputStreamOperator<UserRegister> withWatermarkStream = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserRegister>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                 .withTimestampAssigner(new SerializableTimestampAssigner<UserRegister>() {
                                     @Override
                                     public long extractTimestamp(UserRegister element, long recordTimestamp) {
                                         return DateFormatUtils.dateTimeStringToTs(element.getCreateTime());
                                     }
                                 })
        );
        
        // 开窗
        AllWindowedStream<UserRegister, TimeWindow> windowedStream =
                withWatermarkStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));
        
        // 聚合
        SingleOutputStreamOperator<UserRegister> reducedStream = windowedStream.reduce(new ReduceFunction<UserRegister>() {
            @Override
            public UserRegister reduce(UserRegister value1, UserRegister value2) throws Exception {
                value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                return value1;
            }
        }, new ProcessAllWindowFunction<UserRegister, UserRegister, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<UserRegister, UserRegister, TimeWindow>.Context context, Iterable<UserRegister> elements, Collector<UserRegister> out) throws Exception {
                TimeWindow window = context.window();
                
                String stt = DateFormatUtils.tsToDateTimeString(window.getStart());
                String edt = DateFormatUtils.tsToDateTimeString(window.getEnd());
                String curDate = DateFormatUtils.tsToDateString(System.currentTimeMillis());
                for (UserRegister userRegister : elements) {
                    userRegister.setStt(stt);
                    userRegister.setEdt(edt);
                    userRegister.setCurDate(curDate);
                    out.collect(userRegister);
                }
            }
        });
        
        SingleOutputStreamOperator<String> mapStream = reducedStream.map(JSON::toJSONString);
        mapStream.sinkTo(FlinkSinkUtils.getDorisSink(Constants.DORIS_TABLE_DWS_USER_USER_REGISTER_WINDOW));
    }
}
