package com.bigdatalighter.kafka.common;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class KafkaOffsetRetrievalFailureException extends Exception{

    public KafkaOffsetRetrievalFailureException(String message) {
        super(message);
    }

}
