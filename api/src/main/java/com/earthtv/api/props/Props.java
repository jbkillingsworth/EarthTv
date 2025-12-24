package com.earthtv.api.props;

import com.earthtv.api.database.IDataBase;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

public class Props implements IProps {

    int collection;
    BigDecimal longitude;
    BigDecimal latitude;
    String id;
    IDataBase database;

    public Props(int collection, BigDecimal longitude,
                 BigDecimal latitude, IDataBase db){
        this.setCollection(collection);
        this.setLongitude(longitude);
        this.setLatitude(latitude);
        this.setDatabase(db);
        this.generateID();
    }

    public void setCollection(int collection){
        this.collection = collection;
    }

    public int getCollection(){
        return this.collection;
    }

    public void setLongitude(BigDecimal longitude){
        this.longitude = longitude;
    }

    public BigDecimal getLongitude(){
        return this.longitude;
    }

    public void setLatitude(BigDecimal latitude){
        this.latitude = latitude;
    }

    public BigDecimal getLatitude(){
        return this.latitude;
    }

    public void setID(String id){
        this.id = id;
    }

    public void generateID(){
        String valToHash = longitude.setScale(5, RoundingMode.DOWN).toString() +
                latitude.setScale(2, RoundingMode.DOWN).toString() +
                String.valueOf(collection);
        byte[] bytes = valToHash.getBytes(StandardCharsets.UTF_8);
        UUID hashedValue = UUID.nameUUIDFromBytes(bytes);
        this.id = hashedValue.toString();
//        this.id = Objects.hash(longitude.setScale(5, RoundingMode.DOWN),
//                latitude.setScale(2, RoundingMode.DOWN), collection);
    }

    public String getID(){
        return this.id;
    }

    public void setDatabase(IDataBase database){
        this.database = database;
//        this.databaseSet = true;
    }

    public IDataBase getDatabase(){
        return this.database;
    }

//    public void update(){
//        if (!this.database.exists(this)){
//            this.database.create(this);
//        }
//    }
}
