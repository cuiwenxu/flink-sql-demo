package com.flinksqldemo.function;

import org.apache.flink.table.functions.ScalarFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

public class CheckTimeBetween extends ScalarFunction {

    public boolean eval(Long ts, String uppper, String lower) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Long event_ts = sdf.parse("1970-01-01 " + sdf.format(new Date(ts)).substring(11, 19)).getTime();
            Long upper_ts = sdf.parse("1970-01-01 " + uppper).getTime() + 60 * 60 * 1000;
            Long lower_ts = sdf.parse("1970-01-01 " + lower).getTime();
            return (event_ts >= lower_ts && event_ts <= upper_ts);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


    public static void main(String[] args) {
        System.out.println(new CheckTimeBetween().eval(1614581245451L, "22:00:00", "10:00:00"));
    }

}

