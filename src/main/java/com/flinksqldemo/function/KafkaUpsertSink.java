package com.flinksqldemo.function;

import com.alibaba.fastjson.JSON;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Properties;

public class KafkaUpsertSink extends RichSinkFunction<HashMap> implements CheckpointedFunction {


    KafkaProducer<String, String> producer = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        Properties properties = new Properties();
//        properties.setProperty("key.serializer", "org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.StringSerializer");
//        properties.setProperty("value.serializer", "org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", "");
        properties.put("enable.auto.commit ", "false");

        producer = new KafkaProducer<String, String>(properties);

    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    @Override
    public void invoke(HashMap value, Context context) throws Exception {
        ProducerRecord<String, String> kafkaMessage = null;
        HashMap recordKeyMap = new HashMap();
        HashMap recordValueMap = new HashMap();
        HashMap keyMap = new HashMap();
        keyMap.put("db", value.get("db").toString());
        keyMap.put("table", value.get("table").toString());
        keyMap.put("id", value.get("id").toString());
        recordKeyMap.put("record_key",keyMap);
        recordValueMap.put("record_key",keyMap);
        recordValueMap.put("record_value",value);
        String recordKey = JSON.toJSONString(recordKeyMap);
        if (value.get("operation").toString().equals("d")) {
            kafkaMessage = new ProducerRecord("topic", recordKey, null);
            System.out.println("Produce ok:" + producer.send(kafkaMessage).get().toString());
        } else if (value.get("operation").toString().equals("u")) {
            //先发tombstone消息
            kafkaMessage = new ProducerRecord("topic", recordKey, null);
            System.out.println("Produce ok:" + producer.send(kafkaMessage).get().toString());
            //再发更改后消息
            kafkaMessage = new ProducerRecord("topic", recordKey, JSON.toJSONString(recordValueMap));
            System.out.println("Produce ok:" + producer.send(kafkaMessage).get().toString());
        } else {
            kafkaMessage = new ProducerRecord("topic", recordKey, JSON.toJSONString(recordValueMap));
            System.out.println("Produce ok:" + producer.send(kafkaMessage).get().toString());
        }
    }

}
