package com.flinksqldemo;

import com.flinksqldemo.function.KafkaUpsertSink;
import com.flinksqldemo.util.MyStringDebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;

public class Binlog2Kafka {


    public void execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MyStringDebeziumDeserializationSchema myStringDebeziumDeserializationSchema = new MyStringDebeziumDeserializationSchema<String>();

        DebeziumSourceFunction<HashMap> debeziumSourceFunction = MySQLSource.builder()
                .hostname("")
                .port(3306)
                .databaseList("")
                .tableList("")
                .username("")
                .password("")
                .deserializer(myStringDebeziumDeserializationSchema).build();

        KafkaUpsertSink kafkaUpsertSink = new KafkaUpsertSink();

        DataStreamSource<HashMap> source = env.addSource(debeziumSourceFunction);
        source.map(new MapFunction<HashMap, HashMap>() {
            @Override
            public HashMap map(HashMap value) throws Exception {
                if (!(value.containsKey("updated_date") || value.containsKey("updated_time"))) {
                    value.put("updated_time", System.currentTimeMillis());
                }
                System.out.println(value.get("table") + "_______________");
                return value;
            }
        })
                .addSink(kafkaUpsertSink);
//                .print();


        env.execute(Binlog2Kafka.class.getName());
    }


    public static void main(String[] args) throws Exception {
        new Binlog2Kafka().execute();
    }


}
