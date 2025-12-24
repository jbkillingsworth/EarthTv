package com.earthtv.api.actions.page;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.earthtv.api.actions.Actions;
import com.earthtv.api.props.IProps;

public class Page extends Actions {

    public Page(IProps item){
        super(item);
    }

    public boolean exists(Connection conn){
        String query_id = this.getProps().getID();
        String sqlQuery = "SELECT * FROM page WHERE query_id='" + query_id + "';";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sqlQuery)) {
            return rs.next();
//            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void create(Connection conn){
        try {
            Statement stmnt = conn.createStatement();
            String query_id = this.getProps().getID();
            String insertQuery = "INSERT INTO page (query_id, status) VALUES ";
            insertQuery = insertQuery + "('" + query_id + "', " + "0" + ")";
            stmnt.executeUpdate(insertQuery);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}