package com.earthtv.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.LocalTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
public class VideoStatusStreamController {
    @GetMapping("/events")
    public SseEmitter eventStream() {
        SseEmitter emitter = new SseEmitter();
        ExecutorService sseMvcExecutor = Executors.newSingleThreadExecutor();
        sseMvcExecutor.execute(() -> {
                executeSseLogic(emitter);
        });
        return emitter;
    }

    private void executeSseLogic(SseEmitter emitter) {
        try {
            for (int counter = 0; counter < 10; counter++) {
                // Create an event with a custom event ID and data
                SseEmitter.SseEventBuilder event = SseEmitter.event().id(String.valueOf(counter))
                        .data("Event data at " + LocalTime.now());
                // Send the event to the client
                emitter.send(event);
                // Wait for one second before sending the next event
                Thread.sleep(1000);
            }
            // Mark the end of the event stream
            emitter.complete();
        } catch (IOException | InterruptedException e) {
            emitter.completeWithError(e);
        }
    }
}