package com.bigdatalighter.kafka.common;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class KafkaTopic {

    private final String name;
    private final List<KafkaPartition> partitions;

    public KafkaTopic(String name, List<KafkaPartition> partitions) {
        this.name = name;
        this.partitions = Lists.newArrayList();
        for (KafkaPartition kafkaPartition : partitions) {
            this.partitions.add(kafkaPartition);
        }
    }

    public String getName() {
        return name;
    }

    public List<KafkaPartition> getPartitions() {
        return partitions;
    }

    @Override
    public String toString() {
        return "KafkaTopic{" +
                "name='" + name + '\'' +
                ", partitions=" + partitions +
                '}';
    }

}
