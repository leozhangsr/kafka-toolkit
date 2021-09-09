package com.bigdatalighter.kafka.offset.reader;

import com.bigdatalighter.kafka.common.KafkaPartitionOffset;
import com.bigdatalighter.kafka.utils.JSONUtils;
import org.apache.curator.framework.CuratorFramework;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class KafkaSpoutOffsetReader extends AbstractZookeeperOffsetReader {

    public KafkaSpoutOffsetReader(CuratorFramework curator, String path) {
        super(curator, path);
    }

    @Override
    public String getOffsetPath(String group, String topic) {
        return mergePath(group, topic);
    }

    @Override
    public KafkaPartitionOffset getPartitionOffset(String topic, String path, byte[] data) throws Exception {
        String jsonstr = new String(data);
        SpoutInfo spoutInfo = JSONUtils.jsonToPojo(jsonstr, SpoutInfo.class);
        return spoutInfo == null ? null : new KafkaPartitionOffset(spoutInfo.getPartition(), spoutInfo.getOffset());
    }

    /**
     * {
     * "topology": {
     * "id": "545d0c40-bce9-4abd-90ea-e34eb2caa432",
     * "name": "topology-name"
     * },
     * "offset": 10001,
     * "partition": 20,
     * "broker": {
     * "host": "10.1.168.136",
     * "port": 9092
     * },
     * "topic": "topic-name"
     * }
     */
    public static class SpoutInfo {
        private Topology topology;
        private HostAndPort broker;
        private String topic;
        private Long offset;
        private Integer partition;


        public static class Topology {
            String id;
            String name;

            public Topology() {
            }

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }
        }

        public static class HostAndPort {
            String host;
            Integer port;

            public String getHost() {
                return host;
            }

            public void setHost(String host) {
                this.host = host;
            }

            public Integer getPort() {
                return port;
            }

            public void setPort(Integer port) {
                this.port = port;
            }
        }

        public Topology getTopology() {
            return topology;
        }

        public void setTopology(Topology topology) {
            this.topology = topology;
        }

        public HostAndPort getBroker() {
            return broker;
        }

        public void setBroker(HostAndPort broker) {
            this.broker = broker;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public Long getOffset() {
            return offset;
        }

        public void setOffset(Long offset) {
            this.offset = offset;
        }

        public Integer getPartition() {
            return partition;
        }

        public void setPartition(Integer partition) {
            this.partition = partition;
        }
    }

}
