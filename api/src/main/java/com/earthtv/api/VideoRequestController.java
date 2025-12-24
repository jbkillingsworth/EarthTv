package com.earthtv.api;
import com.earthtv.api.actions.Actions;
import com.earthtv.api.actions.IActions;
import com.earthtv.api.controller.IController;
import com.earthtv.api.database.DataBase;
import com.earthtv.api.controller.Controller;
import com.earthtv.api.database.IDataBase;
import com.earthtv.api.actions.page.Page;
import com.earthtv.api.props.IProps;
import com.earthtv.api.props.Props;
import com.earthtv.api.actions.video.Video;
import com.earthtv.api.response.IResponse;
import com.earthtv.api.response.Response;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/create-video-request")
@CrossOrigin(origins = "http://localhost:3000")
public class VideoRequestController {

    DataBase db = new DataBase();
    int collection = 0;

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public IResponse createRequest(@RequestParam("longitude") String longitude,
                                   @RequestParam("latitude") String latitude,
                                   @RequestParam("collection") String collection) {
        try{

//            longitude = "-120";
//            latitude = "30";
//            collection = "eo:sentinel-1";

            IProps props = new Props(this.collection, new BigDecimal(longitude), new BigDecimal(latitude), this.db);

            IActions videoActions = new Video(props);
            IActions pageActions = new Page(props);

            List<IActions> actions = new ArrayList<>();
            actions.add(videoActions);
            actions.add(pageActions);

            IController controller = new Controller(actions);
            IController.updateItems(controller);

            KProducer.runProducer(10);
            return new Response(pageActions.getProps(), 0);
        } catch (NumberFormatException e){
            return new Response(-1);
        }
        catch (Exception e) {
            return new Response(-2);
        }

    }

}