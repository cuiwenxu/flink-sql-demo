package com.flinksqldemo;

import com.flinksqldemo.util.KerberosAuth;
import com.alibaba.fastjson.JSON;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.functions.ScalarFunction;


public class CdcDataUseTest {


    public void execute() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name = "myhive";
        String defaultDatabase = "flink_db";
        String hiveConfDir = "";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);

        tableEnv.useCatalog("myhive");
        tableEnv.useDatabase("flink_db");

        tableEnv.createTemporarySystemFunction("getLong", getLong.class);
        tableEnv.createTemporarySystemFunction("getString", getString.class);



        tableEnv.executeSql(
                "insert into cdc_table_hbase\n" +
                        "select record_key,ROW(getString(record_value,'entity_id'))\n" +
                        "from cdc_table  "
        ).print();


    }

    public static class getString extends ScalarFunction {
        public String eval(String input, String key) {
            String value = "";
//            System.out.println(input+")))))))))");
            try {
                value = JSON.parseObject(input).getString(key);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return value;
        }
    }


    public static class getLong extends ScalarFunction {
        public Long eval(String input, String key) {
            Long value = 0L;
//            System.out.println(input+")))))))))");
            try {
                value = Long.parseLong(JSON.parseObject(input).getString(key));
            } catch (Exception e) {
                e.printStackTrace();
            }
            return value;
        }
    }


    public static void main(String[] args) {
        new KerberosAuth().kerberosAuth(false);
        new CdcDataUseTest().execute();
    }


}
