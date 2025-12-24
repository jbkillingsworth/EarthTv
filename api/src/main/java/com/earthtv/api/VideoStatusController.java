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
import com.earthtv.api.response.StatusResponse;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/get-video-status")
@CrossOrigin(origins = "http://localhost:3000")
public class VideoStatusController {

    DataBase db = new DataBase();
    int collection = 0;

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public StatusResponse createRequest(@RequestParam("id") String id) {
        try{
            int status = IActions.getStatus(db.getConnection(), id);
            return new StatusResponse(status);
        } catch (NumberFormatException e){
            return new StatusResponse(-1);
        }
        catch (Exception e) {
            return new StatusResponse(-2);
        }
    }
}