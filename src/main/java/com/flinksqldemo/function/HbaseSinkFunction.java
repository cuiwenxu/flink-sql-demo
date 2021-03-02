package com.flinksqldemo.function;

import com.flinksqldemo.util.HBaseUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;

public class HbaseSinkFunction extends RichSinkFunction<HashMap> {

    private transient Connection connection;
    private transient Long lastInvokeTime;

    // 创建连接
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 获取全局配置文件，并转为ParameterTool
//        ParameterTool params =
//                (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        //创建一个Hbase的连接
//        connection = HBaseUtils.getConnection(
//                params.getRequired("hbase.zookeeper.quorum"),
//                params.get("hbase.zookeeper.property.clientPort", "2181")
//        );
        connection = HBaseUtils.getConnection("xxx", "2181");
    }

    @Override
    public void invoke(HashMap value, Context context) throws Exception {
        ArrayList arrayList = new ArrayList();
        String tableName = value.get("table").toString();
        String entityId = value.get("entity_id").toString()+"#"+value.get("id").toString();
        value.forEach((k, v) -> {
            Pair<String, String> pair = new Pair<String, String>();
            pair.setFirst(k.toString());
            pair.setSecond(v.toString());
            arrayList.add(pair);
        });
        HBaseUtils.putRow(connection, tableName + "_hbase", entityId, "tb_column", arrayList);
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }


}
