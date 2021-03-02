package com.flinksqldemo.function;

import com.alibaba.fastjson.JSON;
import org.apache.flink.table.functions.ScalarFunction;

public class GetJsonLong extends ScalarFunction {
    public Long eval(String input, String key) {
        Long value = 0L;
        try {
            value = Long.parseLong(JSON.parseObject(input).getString(key));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }
}
