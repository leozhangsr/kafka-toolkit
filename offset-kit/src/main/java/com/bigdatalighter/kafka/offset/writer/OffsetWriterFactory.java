package com.bigdatalighter.kafka.offset.writer;

import com.bigdatalighter.kafka.utils.CuratorManager;
import com.google.common.base.Preconditions;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class OffsetWriterFactory {

    public static final String CONSUMER08 = "consumer08";
    public static final String CAMUS = "camus";
    public static final String KAFKA_SPOUT  = "spout";
    public static final String JSON_FILE = "json-file";

    public static IOffsetWriter create(OffsetWriterParams params) {
        switch (params.getType()) {
            case CONSUMER08:
                Preconditions.checkNotNull(params.getZookeeper(), String.format("Zookeeper address is required for type:%s, use --zookeeper or -zk to set Zookeeper address", params.getType()));
                return new Kafka08ConsumerOffsetWriter(CuratorManager.getCurator(params.getZookeeper()), params.getZkPath());
            case CAMUS:
                Preconditions.checkNotNull(params.getZookeeper(), String.format("Zookeeper address is required for type:%s, use --zookeeper or -zk to set Zookeeper address", params.getType()));
                return new CamusOffsetWriter(CuratorManager.getCurator(params.getZookeeper()), params.getZkPath());
            case KAFKA_SPOUT:
                Preconditions.checkNotNull(params.getZookeeper(), String.format("Zookeeper address is required for type:%s, use --zookeeper or -zk to set Zookeeper address", params.getType()));
                return new KafkaSpoutOffsetWriter(CuratorManager.getCurator(params.getZookeeper()), params.getZkPath());
            case JSON_FILE:
                Preconditions.checkNotNull(params.getFi(), String.format("File path is required for type:%s, use -fi to set File input path", params.getType()));
                return new JsonFileOffsetWriter(params.getFi());
            default:
                throw new IllegalArgumentException("Unknown type of offset writer:" + params.getType());
        }
    }

}
