package com.bigdatalighter.kafka.offset.reader;

import com.bigdatalighter.kafka.common.KafkaPartitionOffset;
import org.apache.curator.framework.CuratorFramework;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class Kafka08ConsumerOffsetReader extends AbstractZookeeperOffsetReader {

    public Kafka08ConsumerOffsetReader(CuratorFramework curator, String path) {
        super(curator, path);
    }

    @Override
    public String getOffsetPath(String group, String topic) {
        return mergePath("consumers", group, "offsets", topic);
    }

    @Override
    public KafkaPartitionOffset getPartitionOffset(String topic, String path, byte[] data) throws Exception {
        Integer partitionId = Integer.valueOf(path);
        Long offset = Long.valueOf(new String(data));
        return new KafkaPartitionOffset(partitionId, offset);
    }

}
