package com.flinksqldemo.util;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.Iterator;

public class MyStringDebeziumDeserializationSchema<S> implements DebeziumDeserializationSchema<HashMap> {
    private static final long serialVersionUID = -3168848963265670603L;

    public MyStringDebeziumDeserializationSchema() {
    }

    /**
     * @param record
     * @param out
     * @throws Exception
     */
    public void deserialize(SourceRecord record, Collector<HashMap> out) throws Exception {
        Struct value = (Struct) record.value();
        String opType = value.getString("op");
        System.out.println(opType + "---------------------");

        String db = value.getStruct("source").getString("db");
        String table = value.getStruct("source").getString("table");
        //定义map,先将db、table加进去
        HashMap<String, Object> map = new HashMap<>();
        map.put("db", db);
        map.put("table", table);
        map.put("operation", opType);

        try {
            Struct after = value.getStruct("before");
            //拿到属性schema的迭代器,进行遍历,将kv键值对加入map
            Iterator<Field> iterator = after.schema().fields().iterator();
            while (iterator.hasNext()) {
                String name = iterator.next().name();
                Object o = after.get(name);
                if (name.equals("id")) {
                    map.put(name, o);
                }
            }
        } catch (Exception ex) {

        }

        try {
            Struct after = value.getStruct("after");
            //拿到属性schema的迭代器,进行遍历,将kv键值对加入map
            Iterator<Field> iterator = after.schema().fields().iterator();
            while (iterator.hasNext()) {
                String name = iterator.next().name();
                Object o = after.get(name);
                map.put(name, o);
            }
        } catch (Exception ex) {

        }
        out.collect(map);
    }

    public TypeInformation<HashMap> getProducedType() {
        return TypeInformation.of(HashMap.class);
    }


}

