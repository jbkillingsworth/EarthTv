package com.earthtv.api.props;

import com.earthtv.api.database.IDataBase;
import java.math.BigDecimal;

public interface IProps {
    public void setCollection(int collection);
    public int getCollection();
    public void setLongitude(BigDecimal longitude);
    public BigDecimal getLongitude();
    public void setLatitude(BigDecimal latitude);
    public BigDecimal getLatitude();
    public void setID(String id);
    public void generateID();
    public String getID();
    public void setDatabase(IDataBase database);
    public IDataBase getDatabase();
}
