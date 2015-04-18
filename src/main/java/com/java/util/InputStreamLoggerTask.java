package com.java.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by lijc on 15/4/18.
 */
public class InputStreamLoggerTask implements Runnable{
    private static final Logger log = LoggerFactory.getLogger("weedfs");

    private final BufferedReader reader;

    public InputStreamLoggerTask(InputStream src) {
        this.reader = new BufferedReader(new InputStreamReader(src));
    }

    @Override
    public void run() {
        String line = null;
        while (true) {
            try {
                if (this.reader.ready() && (line = this.reader.readLine()) != null) {
                    log.info(line);
                } else {
                    Thread.sleep(200);
                }
            } catch (IOException | InterruptedException e) {
                log.error("PANIC! Unable to read from WeedFs output", e);
            }
        }
    }
}
