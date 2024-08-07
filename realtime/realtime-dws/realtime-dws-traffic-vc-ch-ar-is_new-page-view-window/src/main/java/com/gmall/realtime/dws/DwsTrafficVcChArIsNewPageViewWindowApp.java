package com.gmall.realtime.dws;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.gmall.realtime.common.base.BaseApp;
import com.gmall.realtime.common.bean.TrafficPageView;
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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
 */
public class DwsTrafficVcChArIsNewPageViewWindowApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTrafficVcChArIsNewPageViewWindowApp().start(10022, 4, "dws-traffic-source-keyword-page-view-window-app", Constants.TOPIC_DWD_TRAFFIC_PAGE);
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
                    String mid = jsonObject.getJSONObject("common").getString("mid");
                    if (ts != null && StringUtils.isNotBlank(mid)) {
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
        
        // 构建TrafficPageView对象
        SingleOutputStreamOperator<TrafficPageView> beanStream = keyedByMidStream.process(new KeyedProcessFunction<String, JSONObject, TrafficPageView>() {
            ValueState<String> lastLoginDtValueState;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("last-login-dt", String.class);
                valueStateDescriptor.enableTimeToLive(
                        StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1L))
                                      .updateTtlOnCreateAndWrite()
                                      .build()
                );
                lastLoginDtValueState = getRuntimeContext().getState(valueStateDescriptor);
            }
            
            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, TrafficPageView>.Context ctx, Collector<TrafficPageView> out) throws Exception {
                JSONObject common = value.getJSONObject("common");
                // app版本号
                String vc = common.getString("vc");
                // 渠道
                String ch = common.getString("ch");
                // 地区
                String ar = common.getString("ar");
                // 新老访客状态标记
                String isNew = common.getString("is_new");
                
                Long ts = value.getLong("ts");
                String curDate = DateFormatUtils.tsToDateString(ts);
                String lastLoginDt = lastLoginDtValueState.value();
                Long uvCt = 0L;
                if (lastLoginDt == null || !lastLoginDt.equals(curDate)) {
                    uvCt = 1L;
                    lastLoginDtValueState.update(curDate);
                }
                
                JSONObject page = value.getJSONObject("page");
                
                Long svCt = 0L;
                String lastPageId = page.getString("last_page_id");
                // lastPageId为空表示新的会话开始
                if (StringUtils.isBlank(lastPageId)) {
                    svCt = 1L;
                }
                
                Long duringTime = page.getLong("during_time");
                String sid = common.getString("sid");
                out.collect(
                        TrafficPageView.builder()
                                       .curDate(curDate)
                                       .vc(vc)
                                       .ch(ch)
                                       .ar(ar)
                                       .isNew(isNew)
                                       .uvCt(uvCt)
                                       .svCt(svCt)
                                       .pvCt(1L)
                                       .durSum(duringTime)
                                       .ts(ts)
                                       .sid(sid)
                                       .build()
                );
            }
        });
        
        // 添加水位线
        SingleOutputStreamOperator<TrafficPageView> withWatermarkStream = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrafficPageView>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                 .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageView>() {
                                     @Override
                                     public long extractTimestamp(TrafficPageView element, long recordTimestamp) {
                                         return element.getTs();
                                     }
                                 })
        );
        
        // 按照粒度分区
        KeyedStream<TrafficPageView, String> keyByByGranularityStream = withWatermarkStream.keyBy(new KeySelector<TrafficPageView, String>() {
            @Override
            public String getKey(TrafficPageView value) throws Exception {
                return value.getVc() + ":" + value.getCh() + ":" + value.getAr() + ":" + value.getIsNew();
            }
        });
        
        // 开窗
        WindowedStream<TrafficPageView, String, TimeWindow> windowedStream =
                keyByByGranularityStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));
        
        // 聚合
        SingleOutputStreamOperator<TrafficPageView> reducedStream = windowedStream.reduce(
                new ReduceFunction<TrafficPageView>() {
                    @Override
                    public TrafficPageView reduce(TrafficPageView value1, TrafficPageView value2) throws Exception {
                        /*
                         * value1: 表示累加结果
                         * value2: 表示新进来的元素
                         *  */
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        return value1;
                    }
                }, new ProcessWindowFunction<TrafficPageView, TrafficPageView, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TrafficPageView, TrafficPageView, String, TimeWindow>.Context context, Iterable<TrafficPageView> elements, Collector<TrafficPageView> out) throws Exception {
                        TimeWindow window = context.window();
                        
                        String stt = DateFormatUtils.tsToDateTimeString(window.getStart());
                        String edt = DateFormatUtils.tsToDateTimeString(window.getEnd());
                        String curDate = DateFormatUtils.tsToDateString(System.currentTimeMillis());
                        for (TrafficPageView trafficPageView : elements) {
                            trafficPageView.setStt(stt);
                            trafficPageView.setEdt(edt);
                            trafficPageView.setCurDate(curDate);
                            out.collect(trafficPageView);
                        }
                    }
                });
        
        SingleOutputStreamOperator<String> mapStream = reducedStream.map(JSON::toJSONString);
        mapStream.sinkTo(FlinkSinkUtils.getDorisSink(Constants.DORIS_TABLE_DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW));
    }
}
