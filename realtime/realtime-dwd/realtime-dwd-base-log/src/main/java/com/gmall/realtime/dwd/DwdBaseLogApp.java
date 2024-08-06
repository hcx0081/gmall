package com.gmall.realtime.dwd;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.LocalDateTimeUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.gmall.realtime.common.base.BaseApp;
import com.gmall.realtime.common.constant.Constants;
import com.gmall.realtime.common.util.DateFormatUtils;
import com.gmall.realtime.common.util.FlinkSinkUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.temporal.ChronoUnit;

/**
 * 日志分流
 */
public class DwdBaseLogApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwdBaseLogApp().start(10011, 4, "dwd-base-log-app", Constants.TOPIC_LOG);
    }
    
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> source) {
        /* 1. 从Kafka中ETL数据 */
        SingleOutputStreamOperator<JSONObject> jsonObjectStream = etlData(source);
        
        /* 2. 修复新旧访客 */
        KeyedStream<JSONObject, String> keyedStream = waterMarkAndKeyBy(jsonObjectStream);
        SingleOutputStreamOperator<JSONObject> fixIsNewStream = fixIsNew(keyedStream);
        
        /* 3. 拆分不同类型的日志 */
        // 页面日志：动作信息、曝光信息、页面信息、错误信息
        // 启动日志：启动信息、错误信息
        OutputTag<String> actionTag = new OutputTag<>("action", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<>("display", TypeInformation.of(String.class));
        OutputTag<String> errTag = new OutputTag<>("err", TypeInformation.of(String.class));
        OutputTag<String> startTag = new OutputTag<>("start", TypeInformation.of(String.class));
        
        SingleOutputStreamOperator<String> pageStream = splitLog(fixIsNewStream, actionTag, displayTag, errTag, startTag);
        DataStream<String> actionStream = pageStream.getSideOutput(actionTag);
        DataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        DataStream<String> errStream = pageStream.getSideOutput(errTag);
        DataStream<String> startStream = pageStream.getSideOutput(startTag);
        
        
        pageStream.print("page");
        actionStream.print("action");
        displayStream.print("display");
        errStream.print("err");
        startStream.print("start");
        
        
        pageStream.sinkTo(FlinkSinkUtils.getKafkaSink(Constants.TOPIC_DWD_TRAFFIC_PAGE));
        actionStream.sinkTo(FlinkSinkUtils.getKafkaSink(Constants.TOPIC_DWD_TRAFFIC_ACTION));
        displayStream.sinkTo(FlinkSinkUtils.getKafkaSink(Constants.TOPIC_DWD_TRAFFIC_DISPLAY));
        errStream.sinkTo(FlinkSinkUtils.getKafkaSink(Constants.TOPIC_DWD_TRAFFIC_ERR));
        startStream.sinkTo(FlinkSinkUtils.getKafkaSink(Constants.TOPIC_DWD_TRAFFIC_START));
    }
    
    private SingleOutputStreamOperator<String> splitLog(SingleOutputStreamOperator<JSONObject> fixIsNewStream, OutputTag<String> actionTag, OutputTag<String> displayTag, OutputTag<String> errTag, OutputTag<String> startTag) {
        return fixIsNewStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                JSONObject err = value.getJSONObject("err");
                if (MapUtils.isNotEmpty(err)) {
                    ctx.output(errTag, err.toJSONString());
                    value.remove("err");
                }
                
                JSONObject start = value.getJSONObject("start");
                JSONObject page = value.getJSONObject("page");
                JSONObject common = value.getJSONObject("common");
                Long ts = value.getLong("ts");
                if (MapUtils.isNotEmpty(start)) {
                    ctx.output(startTag, value.toJSONString());
                    value.remove("start");
                } else if (MapUtils.isNotEmpty(page)) {
                    JSONArray displays = value.getJSONArray("displays");
                    if (CollectionUtils.isNotEmpty(displays)) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("page", page);
                            display.put("ts", ts);
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                    value.remove("displays");
                    
                    JSONArray actions = value.getJSONArray("actions");
                    if (CollectionUtils.isNotEmpty(actions)) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("page", page);
                            action.put("ts", ts);
                            ctx.output(actionTag, action.toJSONString());
                        }
                    }
                    value.remove("actions");
                    
                    // 主流
                    out.collect(value.toJSONString());
                }
            }
        });
    }
    
    private SingleOutputStreamOperator<JSONObject> etlData(DataStreamSource<String> source) {
        return source.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                boolean isJsonObj = JSON.isValidObject(value);
                if (isJsonObj) {
                    JSONObject jsonObject = JSON.parseObject(value);
                    JSONObject page = jsonObject.getJSONObject("page");// 页面浏览日志
                    JSONObject start = jsonObject.getJSONObject("start");// 启动日志
                    JSONObject common = jsonObject.getJSONObject("common");
                    Long ts = jsonObject.getLong("ts");
                    if (MapUtils.isNotEmpty(page) || MapUtils.isNotEmpty(start) &&
                            (MapUtils.isNotEmpty(common) && StringUtils.isNotBlank(common.getString("mid")) && ts != null)) {
                        out.collect(jsonObject);
                    }
                }
            }
        });
    }
    
    private KeyedStream<JSONObject, String> waterMarkAndKeyBy(SingleOutputStreamOperator<JSONObject> jsonObjectStream) {
        // return jsonObjectStream.assignTimestampsAndWatermarks(
        //         WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
        //                          .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
        //                              @Override
        //                              public long extractTimestamp(JSONObject element, long recordTimestamp) {
        //                                  return element.getLong("ts");
        //                              }
        //                          })
        // ).keyBy(new KeySelector<JSONObject, String>() {
        //     @Override
        //     public String getKey(JSONObject value) throws Exception {
        //         return value.getJSONObject("common").getString("mid");
        //     }
        // });
        
        // 测试使用
        return jsonObjectStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                                 .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                     @Override
                                     public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                         return element.getLong("ts");
                                     }
                                 })
        ).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });
    }
    
    private SingleOutputStreamOperator<JSONObject> fixIsNew(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            
            ValueState<String> firstLoginDtState;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                firstLoginDtState = getRuntimeContext().getState(new ValueStateDescriptor<>("first-login-dt", String.class));
            }
            
            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject common = value.getJSONObject("common");
                String isNew = common.getString("is_new");
                String firstLoginDt = firstLoginDtState.value();
                Long ts = value.getLong("ts");
                String currentDt = DateFormatUtils.tsToDateString(ts);
                // 新访客
                if ("1".equals(isNew)) {
                    if (StringUtils.isNotBlank(firstLoginDt) && !firstLoginDt.equals(currentDt)) {
                        // 如果状态不为空、日期不是今天，说明这个访客伪装成为新访客
                        common.put("is_new", "0");
                    } else if (StringUtils.isBlank(firstLoginDt)) {
                        firstLoginDtState.update(currentDt);
                    }
                }
                // 老访客
                if ("0".equals(isNew)) {
                    if (firstLoginDt == null) {
                        // 使用昨天的日期
                        String beforeDt = LocalDateTimeUtil.format(LocalDateTimeUtil.parseDate(currentDt).minus(1, ChronoUnit.DAYS), DatePattern.NORM_DATE_PATTERN);
                        firstLoginDtState.update(beforeDt);
                    }
                }
                out.collect(value);
            }
        });
    }
}