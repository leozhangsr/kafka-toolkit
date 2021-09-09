package com.bigdatalighter.kafka.offset.writer;

import com.bigdatalighter.kafka.common.KafkaPartitionOffset;
import com.bigdatalighter.kafka.offset.reader.Kafka08ConsumerOffsetReader;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.StandardCharsets;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class Kafka08ConsumerOffsetWriter extends AbstractZookeeperOffsetWriter{

    public Kafka08ConsumerOffsetWriter(CuratorFramework curatorFramework, String path) {
        super(curatorFramework, path);
    }

    @Override
    public String getBasePathForPartitions(String group, String topic) throws Exception {
        return mergePath(Kafka08ConsumerOffsetReader.CONSUMERS, group, Kafka08ConsumerOffsetReader.OFFSETS, topic);
    }

    @Override
    public String getNameForPartition(int partition) {
        return String.valueOf(partition);
    }

    @Override
    public byte[] serializeOffset(KafkaPartitionOffset partitionOffset) throws Exception {
        return String.valueOf(partitionOffset.getOffset()).getBytes(StandardCharsets.UTF_8);
    }

}
