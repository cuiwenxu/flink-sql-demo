package com.flinksqldemo.util;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.streaming.connectors.kafka.KafkaContextAware;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Iterator;

public class KafkaSerializationSchema implements org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema<Struct>, KafkaContextAware<String> {


    SimpleStringSchema simpleStringSchema = null;

    @Override
    public void setParallelInstanceId(int parallelInstanceId) {

    }

    public KafkaSerializationSchema(SimpleStringSchema simpleStringSchema) {
        this.simpleStringSchema = simpleStringSchema;
    }

    @Override
    public void setNumParallelInstances(int numParallelInstances) {

    }

    @Override
    public void setPartitions(int[] partitions) {

    }

    @Override
    public String getTargetTopic(String rowData) {
        return null;
    }


    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {

    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Struct record, @Nullable Long aLong) {
        byte[] valueSerialized;

        String opType = record.getString("op");
        System.out.println(opType + "---------------------");

        String dbName = record.getStruct("source").getString("db");
        String table = record.getStruct("source").getString("table");
        //定义map,先将db、table加进去
        HashMap<String, Object> map = new HashMap<>();
        map.put("dbName", dbName);
        map.put("table", table);

        if (opType.equals("d")) {
            valueSerialized = null;
            return new ProducerRecord(dbName, (dbName + table).hashCode(), simpleStringSchema.serialize(table), valueSerialized);
        } else if (opType.equals("c")) {
            try {
                Struct before = record.getStruct("before");
                //拿到属性schema的迭代器,进行遍历,将kv键值对加入map
                Iterator<Field> iterator = before.schema().fields().iterator();
                while (iterator.hasNext()) {
                    String name = iterator.next().name();
                    Object o = before.get(name);
                    map.put(name, o);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            valueSerialized = simpleStringSchema.serialize(JSON.toJSONString(map));
            return new ProducerRecord(dbName, (dbName + table).hashCode(), simpleStringSchema.serialize(table), valueSerialized);
        } else if (opType.equals("u")) {
            try {
                Struct before = record.getStruct("before");
                //拿到属性schema的迭代器,进行遍历,将kv键值对加入map
                Iterator<Field> iterator = before.schema().fields().iterator();
                while (iterator.hasNext()) {
                    String name = iterator.next().name();
                    Object o = before.get(name);
                    map.put(name, o);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            valueSerialized = simpleStringSchema.serialize(JSON.toJSONString(map));
            return new ProducerRecord(dbName, (dbName + table).hashCode(), simpleStringSchema.serialize(table), valueSerialized);
        }
        return null;
    }

}