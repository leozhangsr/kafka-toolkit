package com.bigdatalighter.kafka.offset.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class LoggingOffsetStateObserver extends AbsConsumerOffsetStateObserver {
    private Logger logger = LoggerFactory.getLogger(LoggingOffsetStateObserver.class);

    @Override
    protected void doCaughtUp(ConsumerOffsetState offsetState, ConsumerOffsetState.StateInfo stateInfo) {
        logger.info("Consumer[{}:{}] has caught up to target offset;", offsetState.getGroup(), offsetState.getTopic());
    }

    @Override
    protected void doDelay(ConsumerOffsetState offsetState, ConsumerOffsetState.StateInfo stateInfo) {
        logger.warn("Consumer[{}:{}] has fallen behind target offset.Difference:{}.", offsetState.getGroup(), offsetState.getTopic(), stateInfo.getTotalDifference());
        logger.warn("Partition\tCurrent offset\tConsumer offset\tDifference");
        for (ConsumerOffsetState.PartitionOffsetDifference difference : stateInfo.getDifferences()) {
            logger.warn("{} \t {} \t {} \t {}", difference.getPartitionId(), difference.getCurrentOffset(), difference.getTargetOffset(), difference.getDifference());
        }
    }

}
