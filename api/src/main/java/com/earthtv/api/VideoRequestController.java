package com.earthtv.api;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.earthtv.api.KProducer;

@RestController
@RequestMapping("/create-video-request")
public class VideoRequestController {

    @GetMapping
    public String createRequest() {
        try{
            KProducer.runProducer(10);
            return "Request Queued for Processing";
        } catch (Exception e) {
            return "Request Unable to Process";
        }

    }

}