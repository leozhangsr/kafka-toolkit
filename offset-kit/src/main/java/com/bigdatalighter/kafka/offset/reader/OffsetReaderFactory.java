package com.bigdatalighter.kafka.offset.reader;

import com.bigdatalighter.kafka.utils.CuratorManager;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class OffsetReaderFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetReaderFactory.class);
    public static final String CAMUS = "camus";
    public static final String CONSUMER_08 = "consumer08";
    public static final String CONSUMER_NEW = "consumer-new";
    public static final String SPOUT = "spout";
    public static final String BROKER_LOG = "broker-log";
    public static final String JSON_FILE = "json-file";

    public static IOffsetReader create(OffsetReaderParams params) {
        switch (params.getType()) {
            case CAMUS:
                Preconditions.checkNotNull(params.getZookeeper(), String.format("Zookeeper address is required for type:%s, use --zookeeper or -zk to set Zookeeper address", params.getType()));
                LOGGER.info("Create CamusOffsetReader with: zookeeper:{}, path:{}", params.getZookeeper(), params.getZkPath());
                return new CamusOffsetReader(CuratorManager.getCurator(params.getZookeeper()), params.getZkPath());
            case CONSUMER_08:
                Preconditions.checkNotNull(params.getZookeeper(), String.format("Zookeeper address is required for type:%s, use --zookeeper or -zk to set Zookeeper address", params.getType()));
                LOGGER.info("Create KafkaApiConsumerOffsetReader with: zookeeper:{}, path:{}", params.getZookeeper(), params.getZkPath());
                return new Kafka08ConsumerOffsetReader(CuratorManager.getCurator(params.getZookeeper()), params.getZkPath());
            case SPOUT:
                Preconditions.checkNotNull(params.getZookeeper(), String.format("Zookeeper address is required for type:%s, use --zookeeper or -zk to set Zookeeper address", params.getType()));
                LOGGER.info("Create KafkaSpoutConsumerOffsetReader with: zookeeper:{}, path:{}", params.getZookeeper(), params.getZkPath());
                return new KafkaSpoutOffsetReader(CuratorManager.getCurator(params.getZookeeper()), params.getZkPath());
            case CONSUMER_NEW:
                Preconditions.checkNotNull(params.getBrokers(), String.format("Broker address is required for type:%s, use --broker or -b to set broker address", params.getType()));
                LOGGER.info("Create KafkaNewConsumerOffsetReader with broker:{}", params.getBrokers());
                return new KafkaNewConsumerOffsetReader(params.getBrokers());
            case BROKER_LOG:
                Preconditions.checkNotNull(params.getBrokers(), String.format("Broker address is required for type:%s, use --broker or -b to set broker address", params.getType()));
                LOGGER.info("Create KafkaBrokerLogOffsetReader with broker:{}, from :{} offset", params.getBrokers(), params.getFrom());
                return new KafkaBrokerLogOffsetReader(params.getBrokers(), params.getFrom());
            case JSON_FILE:
                return new JsonFileOffsetReader(params.getFi());
            default:
                throw new IllegalArgumentException("unknown offsetReader:" + params.getType());
        }
    }

}
