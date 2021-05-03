package com.talan.reactive.consumer;

import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;

@Log4j2
@Service
public class LoggerConsumer {

    public Consumer<Object> get() {
        return t -> log.error("Output data: " + t);
    }
}
