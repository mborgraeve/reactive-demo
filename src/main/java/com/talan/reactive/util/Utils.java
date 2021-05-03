package com.talan.reactive.util;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class Utils {

    public static void safeSleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }
}
