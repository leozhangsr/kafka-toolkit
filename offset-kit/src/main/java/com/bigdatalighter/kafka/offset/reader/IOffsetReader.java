package com.bigdatalighter.kafka.offset.reader;

import com.bigdatalighter.kafka.common.KafkaTopicOffset;

import java.io.Closeable;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public interface IOffsetReader extends Closeable {

    public KafkaTopicOffset readOffset(String group, String topic) throws Exception;

}
