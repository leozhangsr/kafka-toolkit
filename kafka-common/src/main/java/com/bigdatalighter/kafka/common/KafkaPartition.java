package com.bigdatalighter.kafka.common;

import com.google.common.net.HostAndPort;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class KafkaPartition {

    private final int id;
    private final String topicName;
    private KafkaLeader leader;

    public KafkaPartition(Builder builder) {
        this.id = builder.id;
        this.topicName = builder.topicName;
        this.leader = new KafkaLeader(builder.id, builder.leaderHostAndPort);
    }

    public KafkaPartition(KafkaPartition other) {
        this.id = other.id;
        this.topicName = other.topicName;
        this.leader = new KafkaLeader(other.leader.id, other.leader.hostAndPort);
    }

    public int getId() {
        return id;
    }

    public String getTopicName() {
        return topicName;
    }

    public KafkaLeader getLeader() {
        return leader;
    }

    public void setLeader(int leaderId, String leaderHost, int leaderPort) {
        this.leader = new KafkaLeader(leaderId, HostAndPort.fromParts(leaderHost, leaderPort));
    }

    @Override
    public String toString() {
        return "KafkaPartition{" +
                "id=" + id +
                ", topicName='" + topicName + '\'' +
                ", leader=" + leader +
                '}';
    }

    public final static class KafkaLeader{
        private final int id;
        private final HostAndPort hostAndPort;

        public KafkaLeader(int id, HostAndPort hostAndPort) {
            this.id = id;
            this.hostAndPort = hostAndPort;
        }

        public int getId() {
            return id;
        }

        public HostAndPort getHostAndPort() {
            return hostAndPort;
        }

        @Override
        public String toString() {
            return "KafkaLeader{" +
                    "id=" + id +
                    ", hostAndPort=" + hostAndPort +
                    '}';
        }
    }

    public static class Builder{
        private int id = 0;
        private String topicName = "";
        private int leaderId = 0;
        private HostAndPort leaderHostAndPort;

        public Builder withId(int id) {
            this.id = id;
            return this;
        }

        public Builder withTopicName(String topicName) {
            this.topicName = topicName;
            return this;
        }

        public Builder withLeaderId(int leaderId) {
            this.leaderId = leaderId;
            return this;
        }

        public Builder withLeaderHostAndPort(String hostPortString) {
            this.leaderHostAndPort = HostAndPort.fromString(hostPortString);
            return this;
        }

        public Builder withLeaderHostAndPort(String host, int port) {
            this.leaderHostAndPort = HostAndPort.fromParts(host, port);
            return this;
        }

        public KafkaPartition build() {
            return new KafkaPartition(this);
        }

    }
}
