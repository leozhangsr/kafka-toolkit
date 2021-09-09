package com.bigdatalighter.kafka.offset.writer;

import com.bigdatalighter.kafka.common.KafkaPartitionOffset;
import org.apache.curator.framework.CuratorFramework;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

/**
 * Creator : leozhang
 * Date : 2019/4/19
 * Time : 12:40
 * Description ：
 */
public class CamusOffsetWriter extends AbstractZookeeperOffsetWriter {

    public CamusOffsetWriter(CuratorFramework curatorFramework, String namespace) {
        super(curatorFramework, namespace);
    }

    @Override
    public String getBasePathForPartitions(String group, String topic) throws Exception {
        return mergePath(topic, group, String.valueOf(System.currentTimeMillis()));
    }

    @Override
    public String getNameForPartition(int partition) {
        return String.valueOf(partition);
    }

    @Override
    public byte[] serializeOffset(KafkaPartitionOffset partitionOffset) throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(partitionOffset.getOffset());
        objectOutputStream.close();
        return byteArrayOutputStream.toByteArray();
    }

}
