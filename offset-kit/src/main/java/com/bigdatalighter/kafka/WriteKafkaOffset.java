package com.bigdatalighter.kafka;

import com.bigdatalighter.kafka.common.KafkaTopicOffset;
import com.bigdatalighter.kafka.offset.reader.JsonFileOffsetReader;
import com.bigdatalighter.kafka.offset.writer.IOffsetWriter;
import com.bigdatalighter.kafka.offset.writer.OffsetWriterFactory;
import com.bigdatalighter.kafka.offset.writer.OffsetWriterParams;
import com.bigdatalighter.kafka.utils.ArgsUtils;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class WriteKafkaOffset {

    public static void main(String[] args) throws Exception {
        OffsetWriterParams params = ArgsUtils.parseArgs(args, OffsetWriterParams.class);
        JsonFileOffsetReader offsetReader = new JsonFileOffsetReader(params.getFi());
        IOffsetWriter offsetWriter = OffsetWriterFactory.create(params);
        KafkaTopicOffset topicOffset = offsetReader.readOffset(null, null);
        offsetWriter.writeOffset(topicOffset);
        offsetReader.close();
        offsetWriter.close();
    }

}
