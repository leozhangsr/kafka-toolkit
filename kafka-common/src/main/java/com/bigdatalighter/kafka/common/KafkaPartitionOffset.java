package com.bigdatalighter.kafka.common;

import com.bigdatalighter.kafka.utils.JSONUtils;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class KafkaPartitionOffset {
    private Integer partition;
    private Long offset;

    public KafkaPartitionOffset(Integer partition, Long offset) {
        this.partition = partition;
        this.offset = offset;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return JSONUtils.toJsonString(this);
    }
}
