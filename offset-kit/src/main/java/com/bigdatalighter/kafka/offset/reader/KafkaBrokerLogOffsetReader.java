package com.bigdatalighter.kafka.offset.reader;

import com.bigdatalighter.kafka.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class KafkaBrokerLogOffsetReader implements IOffsetReader {
    private final Logger logger = LoggerFactory.getLogger(KafkaBrokerLogOffsetReader.class);
    private KafkaWrapper kafkaWrapper;
    private boolean fromLatest;

    public KafkaBrokerLogOffsetReader(String brokers, String from) {
        kafkaWrapper = new KafkaWrapper.Builder().withBrokers(brokers).build();
        fromLatest = "latest".equalsIgnoreCase(from);
    }

    @Override
    public KafkaTopicOffset readOffset(String group, String topic) throws Exception {
        ArrayList<Pattern> topics = new ArrayList<Pattern>();
        topics.add(Pattern.compile(topic));
        List<KafkaTopic> filteredTopics = kafkaWrapper.getFilteredTopics(new ArrayList<>(), topics);
        KafkaTopicOffset kafkaTopicOffset = new KafkaTopicOffset();
        kafkaTopicOffset.setGroup(group);
        kafkaTopicOffset.setName(topic);
        for (KafkaTopic kafkaTopic : filteredTopics) {
            List<KafkaPartition> partitions = kafkaTopic.getPartitions();
            for (KafkaPartition partition : partitions) {
                Long offset = getOffset(partition);
                KafkaPartitionOffset partitionOffset = new KafkaPartitionOffset(partition.getId(), offset);
                kafkaTopicOffset.addPartitionOffset(partitionOffset);
            }
        }
        return kafkaTopicOffset;
    }

    @Override
    public void close() throws IOException {
        kafkaWrapper.close();
    }

    private Long getOffset(KafkaPartition partition) {
        try {
            return fromLatest ? kafkaWrapper.getLatestOffset(partition) : kafkaWrapper.getEarliestOffset(partition);
        }catch (Exception e) {
            logger.error("error when read offset from kafka", e);
        }
        return null;
    }

}
