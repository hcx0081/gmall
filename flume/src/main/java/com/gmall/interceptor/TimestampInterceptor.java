package com.gmall.interceptor;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TimestampInterceptor implements Interceptor {
    @Override
    public void initialize() {
    
    }
    
    // 处理单个Event
    @Override
    public Event intercept(Event event) {
        // 获取Header和Body
        Map<String, String> headers = event.getHeaders();
        System.out.println(headers);
        byte[] body = event.getBody();
        String bodyData = new String(body, StandardCharsets.UTF_8);
        try {
            // 将data转换成json对象
            JSONObject jsonObject = JSONObject.parseObject(bodyData);
            String ts = jsonObject.getString("ts");
            headers.put("timestamp", ts);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return event;
    }
    
    // 处理多个Event
    @Override
    public List<Event> intercept(List<Event> list) {
        Iterator<Event> eventIterator = list.iterator();
        while (eventIterator.hasNext()) {
            Event nextEvent = eventIterator.next();
            Event eventResult = intercept(nextEvent);
            if (eventResult == null) {
                eventIterator.remove();
            }
        }
        return list;
    }
    
    @Override
    public void close() {
    
    }
    
    public static class Builder implements Interceptor.Builder {
        
        @Override
        public Interceptor build() {
            return new TimestampInterceptor();
        }
        
        @Override
        public void configure(Context context) {
        
        }
    }
}
