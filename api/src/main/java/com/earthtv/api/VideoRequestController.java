package com.earthtv.api;

import com.earthtv.api.database.DataBase;
import com.earthtv.api.actions.page.Page;
import com.earthtv.api.props.IProps;
import com.earthtv.api.actions.IActions;
import com.earthtv.api.props.Props;
import com.earthtv.api.response.IResponse;
import com.earthtv.api.response.Response;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import java.math.BigDecimal;
import java.time.Instant;

@RestController
@RequestMapping("/create-video-request")
@CrossOrigin(origins = "http://localhost:3000")
public class VideoRequestController {

    DataBase db = new DataBase();
    int collection = 0;
    double WINDOW_SIZE = 0.001;

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public IResponse createRequest(@RequestParam("longitude") String longitude,
                                   @RequestParam("latitude") String latitude,
                                   @RequestParam("collection") String collection) {
        try{

//            longitude = "-120";
//            latitude = "30";
//            collection = "eo:sentinel-1";

            IProps props = new Props(this.collection, new BigDecimal(longitude), new BigDecimal(latitude), this.db);
            IActions pageActions = new Page(props);

            double doubleLongitude = Double.parseDouble(longitude);
            double doubleLatitude = Double.parseDouble(latitude);

            com.earthtv.protos.Video video = com.earthtv.protos.Video.getDefaultInstance();
            com.earthtv.protos.Video.Builder builder = video.newBuilderForType();
            builder.setVideoId(-1);
            builder.setUserId(-1);
            builder.setStart(-1);
            builder.setEnd(Instant.now().getEpochSecond());
            builder.setLon(doubleLongitude);
            builder.setLat(doubleLatitude);
            builder.setMinLon(doubleLongitude - WINDOW_SIZE);
            builder.setMaxLon(doubleLongitude + WINDOW_SIZE);
            builder.setMinLat(doubleLatitude - WINDOW_SIZE);
            builder.setMaxLat(doubleLatitude + WINDOW_SIZE);
            builder.setStatus(0);

            KProducer.runProducer(builder.build());

            return new Response(pageActions.getProps(), 0);
        } catch (NumberFormatException e){
            return new Response(-1);
        }
        catch (Exception e) {
            return new Response(-2);
        }

    }

}