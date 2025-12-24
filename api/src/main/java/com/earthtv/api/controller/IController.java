package com.earthtv.api.controller;

import com.earthtv.api.actions.Actions;
import com.earthtv.api.actions.IActions;

import java.util.List;

public interface IController {
    public void setItems(List<IActions> items);
    public List<IActions> getItems();
    public static void updateItems(IController controller){
        for (IActions item: controller.getItems()){
            if (!item.exists(item.getProps().getDatabase().getConnection())){
                item.create(item.getProps().getDatabase().getConnection());
            }
        }
    }
}
