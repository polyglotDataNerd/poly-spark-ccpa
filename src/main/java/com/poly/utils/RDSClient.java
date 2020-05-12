package com.poly.utils;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class RDSClient implements Serializable {
    private static final Logger log = LogManager.getLogger(com.poly.utils.RDSClient.class);
    private String host;
    private String uid;
    private String pw;
    private String sql;

    public RDSClient(String host, String uid, String pw, String sql) {
        this.host = host;
        this.uid = uid;
        this.pw = pw;
        this.sql = sql;

    }

    public void execute() {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e1) {
            log.error("Class.forName exception: " + e1);
        }
        try (
                Connection c = DriverManager.getConnection(host, uid, pw);
                Statement sqlstmt = c.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)
        ) {
            sqlstmt.setFetchSize(10000);
            sqlstmt.executeUpdate(sql);
        } catch (Exception e) {
            log.error(e.getCause());
            log.error(e.getMessage());
            log.error(e.getStackTrace().toString());
            System.exit(1);
        }
    }

}
