package com.bigdatalighter.kafka;

import com.bigdatalighter.kafka.common.KafkaTopicOffset;
import com.bigdatalighter.kafka.offset.reader.IOffsetReader;
import com.bigdatalighter.kafka.offset.reader.OffsetReaderFactory;
import com.bigdatalighter.kafka.offset.reader.OffsetReaderParams;
import com.bigdatalighter.kafka.utils.ArgsUtils;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class ReadKafkaOffset {

    public static void main(String[] args) throws Exception {
        OffsetReaderParams offsetReaderParams = ArgsUtils.parseArgs(args, OffsetReaderParams.class);
        IOffsetReader offsetReader = OffsetReaderFactory.create(offsetReaderParams);
        KafkaTopicOffset kafkaTopicOffset = offsetReader.readOffset(offsetReaderParams.getGroup(), offsetReaderParams.getTopic());
        offsetReader.close();
        System.out.println(kafkaTopicOffset);
    }

}
