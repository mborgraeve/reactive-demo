package com.talan.reactive.producer;

import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuples;

import java.time.Duration;

@Service
@Log4j2
public class KafkaProducer {

    public Flux<Integer> get() {
        //generate Fibonacci sequence
        log.warn("executed Kafka");
        Flux<Integer> flx = Flux.generate(
                () -> Tuples.of(0, 1),
                (state, sink) -> {
                    sink.next(state.getT1());
                    log.warn("Generated Fibonnaci value {}", state.getT2());
                    return Tuples.of(state.getT2(), state.getT1() + state.getT2());
                }
        );
        flx = flx.delayElements(Duration.ofSeconds(1));
        return flx;
    }
}
