package com.bigdatalighter.kafka.offset.writer;

import com.bigdatalighter.kafka.common.KafkaPartitionOffset;
import com.bigdatalighter.kafka.common.KafkaTopicOffset;
import com.bigdatalighter.kafka.offset.reader.KafkaSpoutOffsetReader;
import com.bigdatalighter.kafka.utils.JSONUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class KafkaSpoutOffsetWriter extends AbstractZookeeperOffsetWriter {

    public KafkaSpoutOffsetWriter(CuratorFramework curatorFramework, String namespace) {
        super(curatorFramework, namespace);
    }

    @Override
    public String getBasePathForPartitions(String group, String topic) throws Exception {
        return mergePath(group, topic);
    }

    @Override
    public String getNameForPartition(int partition) {
        return "partition_" + partition;
    }

    @Override
    public byte[] serializeOffset(KafkaPartitionOffset partitionOffset) throws Exception {
        throw new UnsupportedOperationException("Information is not enough for KafkaSpout");
    }

    @Override
    public void writeOffset(KafkaTopicOffset kafkaTopicOffset) throws Exception {
        checkOffset(kafkaTopicOffset);
        String offsetNamespace = mergePath(this.namespace, getBasePathForPartitions(kafkaTopicOffset.getGroup(), kafkaTopicOffset.getName()));
        Stat stat = curatorFramework.checkExists().forPath(offsetNamespace);
        if (stat == null) {
            curatorFramework.create().creatingParentsIfNeeded().forPath(offsetNamespace);
        }
        CuratorFramework client = this.curatorFramework.usingNamespace(offsetNamespace.substring(1));
        for (KafkaPartitionOffset offset : kafkaTopicOffset.getOffsets().values()) {
            String partitionPath = mergePath(getNameForPartition(offset.getPartition()));
            if (client.checkExists().forPath(partitionPath) == null) {
                KafkaSpoutOffsetReader.SpoutInfo spoutInfo = new KafkaSpoutOffsetReader.SpoutInfo();
                spoutInfo.setOffset(offset.getOffset());
                spoutInfo.setPartition(offset.getPartition());
                spoutInfo.setTopic(kafkaTopicOffset.getName());
                KafkaSpoutOffsetReader.SpoutInfo.Topology topology = new KafkaSpoutOffsetReader.SpoutInfo.Topology();
                topology.setName(kafkaTopicOffset.getName());
                spoutInfo.setTopology(topology);
                client.create().forPath(partitionPath, JSONUtils.toJsonString(spoutInfo).getBytes(StandardCharsets.UTF_8));
            } else {
                byte[] bytes = client.getData().forPath(partitionPath);
                KafkaSpoutOffsetReader.SpoutInfo spoutInfo = JSONUtils.jsonToPojo(new String(bytes), KafkaSpoutOffsetReader.SpoutInfo.class);
                spoutInfo.setOffset(offset.getOffset());
                client.setData().forPath(partitionPath, JSONUtils.toJsonString(spoutInfo).getBytes(StandardCharsets.UTF_8));
            }
        }
    }
}
