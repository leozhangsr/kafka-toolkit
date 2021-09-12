package com.bigdatalighter.kafka.offset.monitor;

import com.bigdatalighter.kafka.common.KafkaPartitionOffset;
import com.bigdatalighter.kafka.common.KafkaTopicOffset;
import com.bigdatalighter.kafka.offset.reader.IOffsetReader;
import com.bigdatalighter.kafka.utils.JSONUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Observable;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class ConsumerOffsetState extends Observable {
    private final Logger logger = LoggerFactory.getLogger(ConsumerOffsetState.class);
    private IOffsetReader targetOffsetReader;
    private IOffsetReader currentOffsetReader;
    private String group;
    private String topic;

    public ConsumerOffsetState(IOffsetReader targetOffsetReader, IOffsetReader consumerOffsetReader, String group, String topic) {
        this.targetOffsetReader = targetOffsetReader;
        this.currentOffsetReader = consumerOffsetReader;
        this.group = group;
        this.topic = topic;
    }

    public StateInfo checkState() {
        StateInfo consumerState = getConsumerState();
        if (consumerState != null) {
            setChanged();
            notifyObservers(consumerState);
        }
        return consumerState;
    }

    private StateInfo getConsumerState() {
        try {
            KafkaTopicOffset targetOffset = targetOffsetReader.readOffset(group, topic);
            KafkaTopicOffset consumerOffset = currentOffsetReader.readOffset(group, topic);
            return checkDifference(targetOffset, consumerOffset);
        } catch (Exception e) {
            logger.error("Error when check offset state", e);
        }
        return null;
    }

    private StateInfo checkDifference(KafkaTopicOffset targetOffset, KafkaTopicOffset consumerOffset) {
        Map<Integer, KafkaPartitionOffset> consumerOffsets = consumerOffset.getOffsets();
        List<PartitionOffsetDifference> differences = new ArrayList<>(consumerOffsets.size());
        long totalDifference = 0;
        boolean hasCaughtUp = true;
        for (KafkaPartitionOffset targetPartitionOffset : targetOffset.getOffsets().values()) {
            KafkaPartitionOffset consumerPartitionOffset = consumerOffsets.get(targetPartitionOffset.getPartition());
            PartitionOffsetDifference difference = new PartitionOffsetDifference(group, topic, targetPartitionOffset.getPartition(), targetPartitionOffset.getOffset(), consumerPartitionOffset == null ? -1l : consumerPartitionOffset.getOffset());
            differences.add(difference);
            hasCaughtUp &= difference.hasCaughtUp();
            totalDifference += difference.getDifference();
        }
        StateInfo stateInfo = new StateInfo();
        stateInfo.setDifferences(differences);
        stateInfo.setTotalDifference(totalDifference);
        stateInfo.setHasCaughtUp(hasCaughtUp);
        return stateInfo;
    }

    public String getGroup() {
        return group;
    }

    public String getTopic() {
        return topic;
    }

    public static class StateInfo {
        private long totalDifference;
        private boolean hasCaughtUp;
        private List<PartitionOffsetDifference> differences;

        public long getTotalDifference() {
            return totalDifference;
        }

        public void setTotalDifference(long totalDifference) {
            this.totalDifference = totalDifference;
        }

        public boolean hasCaughtUp() {
            return hasCaughtUp;
        }

        public void setHasCaughtUp(boolean hasCaughtUp) {
            this.hasCaughtUp = hasCaughtUp;
        }

        public List<PartitionOffsetDifference> getDifferences() {
            return differences;
        }

        public void setDifferences(List<PartitionOffsetDifference> differences) {
            this.differences = differences;
        }

        @Override
        public String toString() {
            return JSONUtils.toJsonString(this);
        }
    }

    public static class PartitionOffsetDifference {
        private String group;
        private String topic;
        private int partitionId;
        private long targetOffset;
        private long currentOffset;

        public PartitionOffsetDifference() {
        }

        public PartitionOffsetDifference(String group, String topic, int partitionId, long targetOffset, long currentOffset) {
            this.group = group;
            this.topic = topic;
            this.partitionId = partitionId;
            this.targetOffset = targetOffset;
            this.currentOffset = currentOffset;
        }

        public boolean hasCaughtUp() {
            return getDifference() >= 0;
        }

        public long getDifference() {
            return currentOffset - targetOffset;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public void setPartitionId(int partitionId) {
            this.partitionId = partitionId;
        }

        public long getTargetOffset() {
            return targetOffset;
        }

        public void setTargetOffset(long targetOffset) {
            this.targetOffset = targetOffset;
        }

        public long getCurrentOffset() {
            return currentOffset;
        }

        public void setCurrentOffset(long currentOffset) {
            this.currentOffset = currentOffset;
        }

        @Override
        public String toString() {
            return JSONUtils.toJsonString(this);
        }

    }
}
