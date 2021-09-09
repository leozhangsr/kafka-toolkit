package com.bigdatalighter.kafka.offset.writer;

import com.bigdatalighter.kafka.common.KafkaPartitionOffset;
import com.bigdatalighter.kafka.common.KafkaTopicOffset;
import com.bigdatalighter.kafka.utils.CuratorManager;
import com.bigdatalighter.kafka.utils.PathMerger;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Creator : leozhang
 * Date : 2019/4/19
 * Time : 11:57
 * Description ：
 */
public abstract class AbstractZookeeperOffsetWriter implements IOffsetWriter {
    private Logger logger = LoggerFactory.getLogger(AbstractZookeeperOffsetWriter.class);
    protected CuratorFramework curatorFramework;
    protected String namespace;
    private PathMerger pathMerger;

    public AbstractZookeeperOffsetWriter(CuratorFramework curatorFramework, String namespace) {
        this.curatorFramework = curatorFramework;
        this.namespace = namespace;
        CuratorFrameworkState state = curatorFramework.getState();
        logger.info("curator state:{}", state);
        if (!CuratorFrameworkState.STARTED.equals(state)) {
            this.curatorFramework.start();
        }
        pathMerger = new PathMerger();
    }

    @Override
    public void writeOffset(KafkaTopicOffset kafkaTopicOffset) throws Exception {
        checkOffset(kafkaTopicOffset);
        CuratorFramework client = this.curatorFramework.usingNamespace(namespace);
        Map<Integer, KafkaPartitionOffset> offsets = kafkaTopicOffset.getOffsets();
        String basePathForPartitions = getBasePathForPartitions(kafkaTopicOffset.getGroup(), kafkaTopicOffset.getName());
        for (KafkaPartitionOffset offset : offsets.values()) {
            String partitionsName = getNameForPartition(offset.getPartition());
            String offsetPath = mergePath(basePathForPartitions, partitionsName);
            Stat stat = client.checkExists().forPath(offsetPath);
            if (stat == null) {
                client.create().creatingParentsIfNeeded().forPath(offsetPath, serializeOffset(offset));
            } else {
                client.setData().forPath(offsetPath, serializeOffset(offset));
            }
        }
    }

    protected void checkOffset(KafkaTopicOffset kafkaTopicOffset) {
        Preconditions.checkNotNull(kafkaTopicOffset.getGroup(), "consumer group can't be null");
        Preconditions.checkNotNull(kafkaTopicOffset.getName(), "topic name can't be null");
        Preconditions.checkState(kafkaTopicOffset.getOffsets() != null && kafkaTopicOffset.getOffsets().size() > 0, "offsets is empty");
    }

    public abstract String getBasePathForPartitions(String group, String topic) throws Exception;

    public abstract String getNameForPartition(int partition);

    public abstract byte[] serializeOffset(KafkaPartitionOffset partitionOffset) throws Exception;

    protected String mergePath(String... paths) {
        return pathMerger.mergePath(paths);
    }

    @Override
    public void close() throws IOException {
        CuratorManager.close(curatorFramework);
    }

}
