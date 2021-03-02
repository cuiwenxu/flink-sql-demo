package com.flinksqldemo;

import com.flinksqldemo.util.HBaseUtils;
import org.apache.hadoop.hbase.client.Connection;

import java.util.ArrayList;

public class HbaseConnect {


    public static void main(String[] args) {
        ArrayList<String> columns = new ArrayList<String>();
        columns.add("tb_column1");
        Connection connection = HBaseUtils.getConnection("", "2181");
        HBaseUtils.createTable(connection, "xxx", columns);
        HBaseUtils.getAllTables(connection).forEach(x -> System.out.println(x.toString()));

    }

}
