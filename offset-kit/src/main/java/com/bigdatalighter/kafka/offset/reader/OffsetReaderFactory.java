package com.bigdatalighter.kafka.offset.reader;

import com.bigdatalighter.kafka.utils.CuratorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class OffsetReaderFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetReaderFactory.class);
    public static final String CAMUS = "camus";
    public static final String CONSUMER_08 = "consumer08";
    public static final String CONSUMER_NEW = "consumer_new";
    public static final String SPOUT = "spout";
    public static final String BROKER_LOG = "broker_log";

    public static IOffsetReader create(OffsetReaderParams params) {
        switch (params.getType()) {
            case CAMUS:
                LOGGER.info("Create CamusOffsetReader with: zookeeper:{}, path:{}", params.getZookeeper(), params.getZkPath());
                return new CamusOffsetReader(CuratorManager.getCurator(params.getZookeeper()), params.getZkPath());
            case CONSUMER_08:
                LOGGER.info("Create KafkaApiConsumerOffsetReader with: zookeeper:{}, path:{}", params.getZookeeper(), params.getZkPath());
                return new Kafka08ConsumerOffsetReader(CuratorManager.getCurator(params.getZookeeper()), params.getZkPath());
            case SPOUT:
                LOGGER.info("Create KafkaSpoutConsumerOffsetReader with: zookeeper:{}, path:{}", params.getZookeeper(), params.getZkPath());
                return new KafkaSpoutConsumerOffsetReader(CuratorManager.getCurator(params.getZookeeper()), params.getZkPath());
            case CONSUMER_NEW:
                LOGGER.info("Create KafkaNewConsumerOffsetReader with broker:{}, group:{}", params.getBrokers(), params.getGroup());
                return new KafkaNewConsumerOffsetReader(params.getBrokers());
            case BROKER_LOG:
                LOGGER.info("Create KafkaBrokerLogOffsetReader with broker:{}, from :{} offset", params.getBrokers(), params.getFrom());
                return new KafkaBrokerLogOffsetReader(params.getBrokers(), params.getFrom());
            default:
                throw new IllegalArgumentException("unknown offsetReader");
        }
    }

}
