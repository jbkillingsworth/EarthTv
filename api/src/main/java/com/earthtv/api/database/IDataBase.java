package com.earthtv.api.database;

import com.earthtv.api.actions.IActions;
import java.sql.Connection;

public interface IDataBase {
    public Connection createConnection();
    public Connection getConnection();
    public boolean exists(IActions record);
    public void create(IActions record);
}
