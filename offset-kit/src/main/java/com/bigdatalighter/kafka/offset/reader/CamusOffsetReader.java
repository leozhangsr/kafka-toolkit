package com.bigdatalighter.kafka.offset.reader;

import com.bigdatalighter.kafka.common.KafkaPartitionOffset;
import org.apache.curator.framework.CuratorFramework;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Optional;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class CamusOffsetReader extends AbstractZookeeperOffsetReader {

    public CamusOffsetReader(CuratorFramework curator, String path) {
        super(curator, path);
    }

    @Override
    public String getOffsetPath(String group, String topic) {
        try {
            CuratorFramework namespace = curator.usingNamespace(this.namespace);
            String topicPath = mergePath(topic, group);
            List<String> children = namespace.getChildren().forPath(topicPath);
            Optional<Long> max = children.stream().filter(p -> !"current".equals(p)).map(p -> Long.valueOf(p)).max(Long::compareTo);
            return max.isPresent() ? mergePath(topic, group, String.valueOf(max.get())) : null;
        }catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public KafkaPartitionOffset getPartitionOffset(String topic, String path, byte[] data) throws Exception {
        ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(data));
        Long offset = (Long) objectInputStream.readObject();
        Integer partitionId = Integer.valueOf(path);
        return new KafkaPartitionOffset(partitionId, offset);
    }
}
