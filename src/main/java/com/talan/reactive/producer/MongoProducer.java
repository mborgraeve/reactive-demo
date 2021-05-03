package com.talan.reactive.producer;

import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

import java.time.Duration;

import static com.talan.reactive.util.Utils.safeSleep;

@Service
@Log4j2
public class MongoProducer {

    public Mono<String> get(int n) {
        log.warn("executed Mongo for " + n);
        safeSleep(100);

        return Mono.just("Data: " + n).delayElement(Duration.ofSeconds(1))
                .cache(Duration.ofSeconds(30));
    }

    public Flux<String> getStrings() {
        log.warn("executed MongoStrings.");
        safeSleep(150);
        Flux<String> flx = Flux.generate(
                () -> Tuples.of(0, 1),
                (state, sink) -> {
                    sink.next("" + state.getT1());
                    log.warn("Generated Fibonnaci value as String {}", state.getT2());
                    return Tuples.of(state.getT2(), state.getT1() + state.getT2());
                }
        );
        flx = flx.delayElements(Duration.ofSeconds(1));
        return flx;
    }

}
