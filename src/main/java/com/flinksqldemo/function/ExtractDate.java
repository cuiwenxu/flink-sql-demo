package com.flinksqldemo.function;

import org.apache.flink.table.functions.ScalarFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ExtractDate extends ScalarFunction {

    public String eval(Long ts, String format) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            return sdf.format(new Date(ts));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }


    public static void main(String[] args) {
        System.out.println(new ExtractDate().eval(1614581245451L,"yyyy-MM-dd HH:mm:ss"));
        System.out.println(new ExtractDate().eval(1614581245451L,"yyyy-MM-dd"));
    }
}
