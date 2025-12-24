package com.earthtv.api.response;

import java.math.BigDecimal;

public interface IResponse {
    public BigDecimal getLongitude();
    public BigDecimal getLatitude();
    public int getCollection();
    public int getStatus();
    public String getId();
}
