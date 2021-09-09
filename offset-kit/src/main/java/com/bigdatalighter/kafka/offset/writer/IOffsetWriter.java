package com.bigdatalighter.kafka.offset.writer;

import com.bigdatalighter.kafka.common.KafkaTopicOffset;

import java.io.Closeable;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public interface IOffsetWriter extends Closeable {

    public void writeOffset(KafkaTopicOffset offset) throws Exception;

}
