package com.earthtv.api.controller;

import java.util.List;
import com.earthtv.api.actions.Actions;
import com.earthtv.api.actions.IActions;

public class Controller implements IController{

    List<IActions> items;

    public Controller(List<IActions> items){
        this.setItems(items);
    }

    public Controller(){
    }

    public void setItems(List<IActions> items){
        this.items = items;
    }

    public List<IActions> getItems(){
        return this.items;
    }


//    public void setEntryID(BigDecimal longitude, BigDecimal latitude, int collection){
//        this.id = Objects.hash(longitude.setScale(5, RoundingMode.DOWN),
//                latitude.setScale(2, RoundingMode.DOWN), collection);
//    }
//
//    public int getEntryID(){
//        return this.id;
//    }

//    public void setRecords(List<Actions> records){
//        this.records = records;
//    }
//
//    public List<Actions> getRecords(){
//        return this.records;
//    }
//
//    public void configureItems(){
//        for (Actions item: this.records){
//            item.setLongitude(this.longitude);
//            item.setLatitude(this.latitude);
//            item.setCollection(this.collection);
//            item.setID(this.id);
//            item.setDatabase(this.database);
//        }
//    }
//
//    public void update(){
//        for (Props item: this.items){
//            item.update();
//        }
//    }
}
