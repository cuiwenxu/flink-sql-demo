package com.flinksqldemo.function;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.util.HashMap;

public class HiveTableAssigner implements BucketAssigner<HashMap, String> {
    @Override
    public String getBucketId(HashMap element, Context context) {
        String tbName = element.get("table").toString();
        return tbName + "_cdc_hive";
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
