package com.example.kafka.spring.controller;

import com.example.kafka.spring.producer.NumberProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Slf4j
@Controller
@RequiredArgsConstructor
public class KafkaController {
    private final NumberProducer numberProducer;

    @GetMapping("/produce")
    public ResponseEntity procudeNumbers(@RequestParam Integer number) {
        log.info("Producer endpoint is called with number: " + number);
        numberProducer.sendNumbers(number);
        return ResponseEntity.ok().build();
    }
}
