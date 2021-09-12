package com.bigdatalighter.kafka.offset.monitor;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class OffsetStateObserverFactory {
    public static final String LOG = "log";

    private OffsetStateObserverFactory() {
    }

    public static AbsConsumerOffsetStateObserver create(OffsetStateObserverParams params) {
        switch (params.getType()) {
            case LOG:
                return new LoggingOffsetStateObserver();
            default:
                throw new IllegalArgumentException("unKnown ConsumerOffsetStateObserver type");
        }
    }
}
