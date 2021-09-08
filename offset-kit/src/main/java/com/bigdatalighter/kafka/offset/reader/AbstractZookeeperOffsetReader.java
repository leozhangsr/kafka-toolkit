package com.bigdatalighter.kafka.offset.reader;

import com.bigdatalighter.kafka.common.KafkaPartitionOffset;
import com.bigdatalighter.kafka.common.KafkaTopicOffset;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;


/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public abstract class AbstractZookeeperOffsetReader implements IOffsetReader {
    protected final Logger logger = LoggerFactory.getLogger(AbstractZookeeperOffsetReader.class);
    public static final String PATH_DELIMITER = "/";
    protected CuratorFramework curator;
    protected String namespace;
    private StringBuilder pathMerger;

    public AbstractZookeeperOffsetReader(CuratorFramework curator, String namespace) {
        this.curator = curator;
        this.namespace = namespace;
        CuratorFrameworkState state = curator.getState();
        logger.info("curator state:{}", state);
        if (!CuratorFrameworkState.STARTED.equals(state)) {
            this.curator.start();
        }
        pathMerger = new StringBuilder();
    }

    @Override
    public KafkaTopicOffset readOffset(String group, String topic) throws Exception {
        KafkaTopicOffset kafkaTopicOffset = new KafkaTopicOffset();
        kafkaTopicOffset.setGroup(group);
        kafkaTopicOffset.setName(topic);
        try {
            CuratorFramework curatorFramework = curator.usingNamespace(namespace);
            String offsetPath = getOffsetPath(group, topic);
            logger.debug("Using namespace {}, and query for path: {}", namespace, offsetPath);
            if (offsetPath != null) {
                List<String> children = curatorFramework.getChildren().forPath(offsetPath);
                logger.debug("Get {} children for namespace {},path: {}", children.size(), namespace, offsetPath);
                for (String child : children) {
                    byte[] data = curatorFramework.getData().forPath(mergePath(offsetPath, child));
                    KafkaPartitionOffset partitionOffset = getPartitionOffset(topic, child, data);
                    kafkaTopicOffset.addPartitionOffset(partitionOffset);
                }
            }
        } catch (Exception e) {
            logger.error("error", e);
        }
        return kafkaTopicOffset;
    }

    protected String mergePath(String... paths) {
        for (int i = 0; i < paths.length; i++) {
            if (i == 0 && paths[i].startsWith(PATH_DELIMITER)) {
                pathMerger.append(paths[i]);
            } else {
                pathMerger.append(PATH_DELIMITER).append(paths[i]);
            }
        }
        String path = pathMerger.toString();
        pathMerger.delete(0, pathMerger.length());
        return path;
    }

    public abstract String getOffsetPath(String group, String topic);

    public abstract KafkaPartitionOffset getPartitionOffset(String topic, String path, byte[] data) throws Exception;

    @Override
    public void close() throws IOException {
        this.curator.close();
    }

}
