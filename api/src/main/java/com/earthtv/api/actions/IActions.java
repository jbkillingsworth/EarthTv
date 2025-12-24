package com.earthtv.api.actions;

import com.earthtv.api.controller.IController;
import com.earthtv.api.props.IProps;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public interface IActions {
    public IProps getProps();
    public boolean exists(Connection conn);
    public void create(Connection conn);
    public static int getStatus(Connection conn, String id){
        String sqlQuery = "SELECT * FROM video WHERE query_id='" + id + "';";
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sqlQuery);
            if (rs.next()) {
                int status = rs.getInt("status");
                return status;
            }
            return -1;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
