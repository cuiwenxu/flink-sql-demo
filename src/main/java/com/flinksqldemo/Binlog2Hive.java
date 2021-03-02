package com.flinksqldemo;

import com.flinksqldemo.function.HiveTableAssigner;
import com.flinksqldemo.util.KerberosAuth;
import com.flinksqldemo.util.MyStringDebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.HashMap;

public class Binlog2Hive {


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

        // define sink
        RollingPolicy<HashMap, String> rollingPolicy = DefaultRollingPolicy
                .create()
                .withRolloverInterval(60 * 1000L)    // 滚动写入新文件的时间，设置为 1min
                .withMaxPartSize(1024 * 1024 * 128L)    // 设置每个文件的最大大小, 设置为 128M
                .withInactivityInterval(30 * 60 * 1000L)  // 多长时间空闲就写入新文件，这里设置30min
                .build();
        StreamingFileSink<HashMap> sink = StreamingFileSink
                .forRowFormat(new Path("hdfs:///user/hive/warehouse/flink_db.db/"), new SimpleStringEncoder<HashMap>("UTF-8"))   // 数据都写到同一个路径
                .withBucketAssigner(new HiveTableAssigner())
                .withRollingPolicy(rollingPolicy)
                .withBucketCheckInterval(60 * 1000L)  // 桶检查间隔，设置为1min
                .build();


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
                .addSink(sink)
                .setParallelism(1);
//                .print();


        env.execute(Binlog2Hive.class.getName());
    }


    public static void main(String[] args) throws Exception {
        new KerberosAuth().kerberosAuth(false);
        new Binlog2Hive().execute();
    }


}
