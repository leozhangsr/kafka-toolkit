package com.bigdatalighter.kafka.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class ShutdownHookThread extends Thread {
    Logger logger = LoggerFactory.getLogger(ShutdownHookThread.class);
    private volatile boolean hasShutDown = false;
    private static AtomicInteger shutdownTimes = new AtomicInteger(0);
    private final Callable callable;

    public ShutdownHookThread(String name, Callable callable) {
        super("ShutdownHookThread-" + name);
        this.callable = callable;
    }

    @Override
    public void run() {
        synchronized (this) {
            if (!hasShutDown) {
                hasShutDown = true;
                try {
                    callable.call();
                }catch (Exception e) {
                    logger.error("Error when execute shutdown hook task", e);
                }
            }
        }
    }

}
