package com.gmall.realtime.dws;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.gmall.realtime.common.base.BaseApp;
import com.gmall.realtime.common.bean.TrafficHomeDetailPageView;
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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 流量域首页-详情页页面浏览各窗口汇总表
 */
public class DwsTrafficHomeDetailPageViewWindowApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTrafficHomeDetailPageViewWindowApp().start(10023, 4, "dws-traffic-home-detail-page-view-window-app", Constants.TOPIC_DWD_TRAFFIC_PAGE);
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> source) {
        // 过滤脏数据
        SingleOutputStreamOperator<JSONObject> jsonObjectStream = source.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                boolean isJsonObj = JSON.isValidObject(value);
                if (isJsonObj) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String mid = jsonObject.getJSONObject("common").getString("mid");
                    String pageId = jsonObject.getJSONObject("page").getString("page_id");
                    if (StringUtils.isNotBlank(mid) && ("home".equals(pageId) || "good_detail".equals(pageId))) {
                        out.collect(jsonObject);
                    }
                }
            }
        });
        
        // 根据mid进行分区
        KeyedStream<JSONObject, String> keyedByMidStream = jsonObjectStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });
        
        // 判断独立访客数
        SingleOutputStreamOperator<TrafficHomeDetailPageView> beanStream = keyedByMidStream.process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageView>() {
            ValueState<String> homeLastLoginDtValueState;
            ValueState<String> goodDetailLastLoginDtValueState;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> homelastLoginDtValueStateDescriptor = new ValueStateDescriptor<>("home-last-login-dt", String.class);
                homelastLoginDtValueStateDescriptor.enableTimeToLive(
                        StateTtlConfig.newBuilder(Time.days(1L))
                                      .updateTtlOnCreateAndWrite()
                                      .build()
                );
                homeLastLoginDtValueState = getRuntimeContext().getState(homelastLoginDtValueStateDescriptor);
                
                ValueStateDescriptor<String> goodDetaillastLoginDtValueStateDescriptor = new ValueStateDescriptor<>("good-detail-last-login-dt", String.class);
                goodDetaillastLoginDtValueStateDescriptor.enableTimeToLive(
                        StateTtlConfig.newBuilder(Time.days(1L))
                                      .updateTtlOnCreateAndWrite()
                                      .build()
                );
                goodDetailLastLoginDtValueState = getRuntimeContext().getState(goodDetaillastLoginDtValueStateDescriptor);
            }
            
            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageView>.Context ctx, Collector<TrafficHomeDetailPageView> out) throws Exception {
                String pageId = value.getJSONObject("page").getString("page_id");
                Long ts = value.getLong("ts");
                String curDate = DateFormatUtils.tsToDateString(ts);
                
                Long homeUvCt = 0L;
                Long goodDetailUvCt = 0L;
                if ("home".equals(pageId)) {
                    String homeLastLoginDt = homeLastLoginDtValueState.value();
                    if (StringUtils.isBlank(homeLastLoginDt) || !homeLastLoginDt.equals(curDate)) {
                        homeUvCt = 1L;
                        homeLastLoginDtValueState.update(curDate);
                    }
                }
                if ("good_detail".equals(pageId)) {
                    String goodDetailLastLoginDt = goodDetailLastLoginDtValueState.value();
                    if (StringUtils.isBlank(goodDetailLastLoginDt) || !goodDetailLastLoginDt.equals(curDate)) {
                        goodDetailUvCt = 1L;
                        goodDetailLastLoginDtValueState.update(curDate);
                    }
                }
                
                if (homeUvCt + goodDetailUvCt != 0) {
                    out.collect(
                            TrafficHomeDetailPageView.builder()
                                                     .homeUvCt(homeUvCt)
                                                     .goodDetailUvCt(goodDetailUvCt)
                                                     .ts(ts)
                                                     .curDate(curDate)
                                                     .build()
                    );
                }
            }
        });
        
        // 添加水位线
        SingleOutputStreamOperator<TrafficHomeDetailPageView> withWatermarkStream = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrafficHomeDetailPageView>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                 .withTimestampAssigner(new SerializableTimestampAssigner<TrafficHomeDetailPageView>() {
                                     @Override
                                     public long extractTimestamp(TrafficHomeDetailPageView element, long recordTimestamp) {
                                         return element.getTs();
                                     }
                                 })
        );
        
        // 开窗
        AllWindowedStream<TrafficHomeDetailPageView, TimeWindow> windowedStream =
                withWatermarkStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));
        
        // 聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageView> reducedStream = windowedStream.reduce(new ReduceFunction<TrafficHomeDetailPageView>() {
            @Override
            public TrafficHomeDetailPageView reduce(TrafficHomeDetailPageView value1, TrafficHomeDetailPageView value2) throws Exception {
                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                return value1;
            }
        }, new AllWindowFunction<TrafficHomeDetailPageView, TrafficHomeDetailPageView, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageView> values, Collector<TrafficHomeDetailPageView> out) throws Exception {
                
                String stt = DateFormatUtils.tsToDateTimeString(window.getStart());
                String edt = DateFormatUtils.tsToDateTimeString(window.getEnd());
                String curDate = DateFormatUtils.tsToDateString(System.currentTimeMillis());
                for (TrafficHomeDetailPageView trafficHomeDetailPageView : values) {
                    trafficHomeDetailPageView.setStt(stt);
                    trafficHomeDetailPageView.setEdt(edt);
                    trafficHomeDetailPageView.setCurDate(curDate);
                    out.collect(trafficHomeDetailPageView);
                }
            }
        });
        
        SingleOutputStreamOperator<String> mapStream = reducedStream.map(JSON::toJSONString);
        mapStream.sinkTo(FlinkSinkUtils.getDorisSink(Constants.DORIS_TABLE_DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW));
    }
}
