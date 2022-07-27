package com.talan.reactive.controller;

import com.talan.reactive.consumer.LoggerSubscriber;
import com.talan.reactive.producer.KafkaProducer;
import com.talan.reactive.producer.MongoProducer;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Random;

import static com.talan.reactive.util.Utils.safeSleep;

@Service
@Log4j2
public class Controller {

    private final KafkaProducer kafka;
    private final MongoProducer mongo;
    private final LoggerSubscriber subscriber;
    private final Random random = new Random();

    public Controller(KafkaProducer producer, MongoProducer mongo, LoggerSubscriber subscriber) {
        this.kafka = producer;
        this.mongo = mongo;
        this.subscriber = subscriber;
    }

    private static String mergeIntAndStrings(Object a, Object b) {
        return "Data:" + a + ", " + b;
    }

    /*
     Elements are matched one to one and processed as a whole

     Mongo >---|
               |
               v
     Kafa >---1-1----> map >---> output

     */
//    @PostConstruct
    public void planAndSubscribeZip() {
        Flux.zip(kafka.get(), mongo.getStrings())
                .delayElements(Duration.ofSeconds(1))
                .map(t -> {
                    log.warn(t);
                    safeSleep(200);
                    return t;
                })
                .log()
                .subscribe(subscriber.get());
        log.info("Flux definition finished.");
    }

    /*
      Elements from Kafka are mapped, then matched against Mongo data. We need mapped data to do the Mongo call. Mapping occurs before thus can occur even if no elements from mongo are emitted

      Mongo >-------------|
                          |
                          v
      Kafa >---> map >---1-1-------> output

     */
//    @PostConstruct
    public void planAndSubscribeZipWith() {
        kafka.get()
                .delayElements(Duration.ofSeconds(1))
                .map(t -> {
                    log.warn(t);
                    safeSleep(200);
                    return t;
                })
                .flatMap(mongo::get)
                .log()
                .subscribe(subscriber.get());
    }

    /*
      Elements are matched, but not exactly one to one. Elements from Mongo can come faster, they are discarded, and kafak elements are matched with the last element from Mongo

      Mongo >----|
                 |
                 v
      Kafka >--1-0..1----> map >---> output

     */
//    @PostConstruct
    public void planAndSubscribeWithLatest() {
        kafka.get().withLatestFrom(mongo.getStrings(), Controller::mergeIntAndStrings)
                .map(t -> {
                    log.warn(t);
                    safeSleep(200);
                    return t;
                })
                .log()
                .subscribe(subscriber.get());
    }

    /*
      Elements are parallelized before the map

      Mongo >----|
                 |
                 v
      Kafa >--1-0..1--P--> map >---> output
                      |--> map >---> output
                      ...
     */

    @PostConstruct
    public void planAndSubscribeParallelElastic() {
        Flux.zip(kafka.get(), mongo.getStrings())
                .parallel(8)
                .runOn(Schedulers.boundedElastic())
                .map(t -> {
                    safeSleep(7500 + random.nextInt(7000));
                    return t;
                })
                .subscribe(subscriber.get());
    }
}
