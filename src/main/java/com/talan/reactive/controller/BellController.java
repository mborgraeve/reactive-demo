package com.talan.reactive.controller;

import com.talan.reactive.consumer.LoggerSubscriber;
import com.talan.reactive.producer.KafkaProducer;
import com.talan.reactive.producer.MongoProducer;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Random;

import static com.talan.reactive.util.Utils.safeSleep;

@Service
@Log4j2
public class BellController {
    private final KafkaProducer kafka;
    private final MongoProducer mongo;
    private final LoggerSubscriber subscriber;
    private final Random random = new Random();
    private static String mergeIntAndStrings(Object a, Object b) {
        return "Data:" + a + ", " + b;
    }

    public BellController(KafkaProducer producer, MongoProducer mongo, LoggerSubscriber subscriber) {
        this.kafka = producer;
        this.mongo = mongo;
        this.subscriber = subscriber;
    }

//    @PostConstruct
    public void uniqueFlux() {
        kafka.get()
                .map(this::process)
                .map(this::longProcess)
                .flatMap(mongo::get)
                .map(this::process)
                .log()
                .subscribe(subscriber.get());
        log.info("Flux definition finished.");
    }

    @PostConstruct
    public void twoFlux() {
        kafka.get()
                .map(this::process)
                .map(this::longProcess)
                .map(this::getBlockingMongo)
                .map(this::process)
                .log()
                .subscribe(subscriber.get());
        log.info("Flux definition finished.");
    }

    public <T> T process(T object){
        log.info("Sleeping for "+object);
        safeSleep(200);
        return object;
    }

    public <T> T longProcess(T object){
        log.info("Long access for "+object);
        safeSleep(2000);
        return object;
    }

    @SneakyThrows
    public String getBlockingMongo(int n){
        return mongo.get(n)
                //nasty part, + error handling to be done.
                .toFuture().get()
                ;
    }
}
