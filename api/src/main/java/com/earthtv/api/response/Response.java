package com.earthtv.api.response;

import com.earthtv.api.controller.IController;
import com.earthtv.api.props.IProps;

import java.math.BigDecimal;

public class Response implements IResponse {

    IProps props;
    int status;

    public Response(IProps props, int status) {
        this.props = props;
        this.status = status;
    }

    public Response(int status) {
        this.status = status;
    }

    @Override
    public BigDecimal getLongitude() {
        return this.props.getLongitude();
    }

    @Override
    public BigDecimal getLatitude() {
        return this.props.getLatitude();
    }

    @Override
    public int getCollection() {
        return this.props.getCollection();
    }

    @Override
    public int getStatus() {
        return this.status;
    }

    @Override
    public String getId() {
        return this.props.getID();
    }
}
