package com.gmall.realtime.common.schema;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MySimpleStringSchema implements DeserializationSchema<String> {
    @Override
    public String deserialize(byte[] message) throws IOException {
        if (message != null && message.length != 0) {
            return new String(message, StandardCharsets.UTF_8);
        } else {
            return "";
        }
    }
    
    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }
    
    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
