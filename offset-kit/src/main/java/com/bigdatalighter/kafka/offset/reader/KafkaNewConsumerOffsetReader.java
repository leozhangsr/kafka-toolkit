package com.bigdatalighter.kafka.offset.reader;

import com.bigdatalighter.kafka.common.KafkaPartitionOffset;
import com.bigdatalighter.kafka.common.KafkaTopicOffset;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class KafkaNewConsumerOffsetReader implements IOffsetReader {
    private Logger logger = LoggerFactory.getLogger(KafkaNewConsumerOffsetReader.class);
    private AdminClient adminClient;

    public KafkaNewConsumerOffsetReader(String brokers) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokers);
        adminClient = AdminClient.create(props);
    }

    @Override
    public KafkaTopicOffset readOffset(String group, String topic) {
        KafkaTopicOffset kafkaTopicOffset = new KafkaTopicOffset();
        kafkaTopicOffset.setGroup(group);
        kafkaTopicOffset.setName(topic);
        try {
            DescribeTopicsResult topics = adminClient.describeTopics(Lists.newArrayList(topic));
            TopicDescription topicDescription = topics.values().get(topic).get();
            if (topicDescription != null) {
                List<TopicPartition> topicPartitions = topicDescription.partitions().stream().map(t -> new TopicPartition(topic, t.partition())).collect(Collectors.toList());
                ListConsumerGroupOffsetsResult groupOffsets = adminClient.listConsumerGroupOffsets(group, new ListConsumerGroupOffsetsOptions().topicPartitions(topicPartitions));
                Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = groupOffsets.partitionsToOffsetAndMetadata().get();
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetAndMetadataMap.entrySet()) {
                    OffsetAndMetadata metadata = entry.getValue();
                    if (metadata != null) {
                        kafkaTopicOffset.addPartitionOffset(new KafkaPartitionOffset(entry.getKey().partition(), metadata.offset()));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error then read offset " + topic, e);
        }
        return kafkaTopicOffset;
    }

    @Override
    public void close() throws IOException {
        adminClient.close();
    }
}
