package com.bigdatalighter.kafka.common;

import com.bigdatalighter.kafka.utils.JSONUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class KafkaTopicOffset {
    private String group;
    private String name;
    private Map<Integer, KafkaPartitionOffset> offsets;

    public KafkaTopicOffset() {
        offsets = new HashMap<>();
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<Integer, KafkaPartitionOffset> getOffsets() {
        return offsets;
    }

    public void setOffsets(Map<Integer, KafkaPartitionOffset> offsets) {
        this.offsets = offsets;
    }

    public void addPartitionOffset(KafkaPartitionOffset offset) {
        offsets.put(offset.getPartition(), offset);
    }

    public KafkaPartitionOffset getPartitionOffset(Long id) {
        return offsets.get(id);
    }

    @Override
    public String toString() {
        return JSONUtils.toJsonString(this);
    }

}
