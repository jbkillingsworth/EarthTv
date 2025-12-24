package com.earthtv.api.actions;

import com.earthtv.api.props.IProps;

abstract public class Actions implements IActions {

    IProps props;

    public Actions(IProps props){
        this.props = props;
    }

    public IProps getProps(){
        return this.props;
    }
}
