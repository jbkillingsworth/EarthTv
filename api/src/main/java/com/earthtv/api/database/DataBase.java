package com.earthtv.api.database;

import com.earthtv.api.actions.IActions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DataBase implements IDataBase {
    Connection conn;
    String url;
    String user;
    String password;

    public DataBase(){
        this.url = "jdbc:postgresql://localhost:5432/postgres";
        this.user = "postgres";
        this.password = "postgres";
        this.conn = this.createConnection();
    }

    public Connection createConnection() {
        try {
            return DriverManager.getConnection(this.url, this.user, this.password);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Connection getConnection() {
        return this.conn;
    }

    public boolean exists(IActions record) {
        return record.exists(this.conn);
    }

    public void create(IActions record) {
        try{
            record.create(this.conn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
