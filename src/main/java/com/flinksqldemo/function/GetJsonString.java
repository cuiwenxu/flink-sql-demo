package com.flinksqldemo.function;

import com.alibaba.fastjson.JSON;
import org.apache.flink.table.functions.ScalarFunction;

public class GetJsonString extends ScalarFunction {

    public String eval(String input, String key) {
        String value = "";
        try {
            value = JSON.parseObject(input).getString(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }

}