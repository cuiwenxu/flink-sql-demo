package com.flinksqldemo.util;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;

public class KerberosAuth {
    public void kerberosAuth(Boolean debug) {
        try {
            Configuration conf = new org.apache.hadoop.conf.Configuration();
            System.setProperty("java.security.krb5.conf", "krb5.conf");
            conf.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("xxx@xxx", "bigdata.keytab");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}