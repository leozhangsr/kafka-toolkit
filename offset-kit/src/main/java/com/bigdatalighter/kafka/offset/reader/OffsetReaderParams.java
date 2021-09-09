package com.bigdatalighter.kafka.offset.reader;

import com.bigdatalighter.kafka.utils.JSONUtils;
import org.kohsuke.args4j.Option;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class OffsetReaderParams {
    @Option(name="--type", aliases = "-t",  usage = "Type of OffsetReader,known type:consumer08,consumer-new,broker-log,spout,camus,json-file", required = true)
    private String type;
    @Option(name="--zookeeper", aliases = "-zk", usage = "Zookeeper connection in the form host:port")
    private String zookeeper;
    @Option(name="--zk-path", usage = "Namespace path for zookeeper")
    private String zkPath;
    @Option(name="--topic", aliases = "-tp", usage = "Topic to read", required = true)
    private String topic;
    @Option(name = "--brokers", aliases = "-b", usage = "Kafka brokers in the form host:port")
    private String brokers;
    @Option(name = "--group" , aliases = "-g" , usage = "Consumer group", required = true)
    private String group;
    @Option(name = "--from", aliases = "-f", usage = "From earliest/latest offset, only supported by type:broker_log")
    private String from = "latest";
    @Option(name = "--fi", usage = "File to read from")
    private String fi;
    @Option(name = "-fo", usage = "File to store offset")
    private String fo;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getZookeeper() {
        return zookeeper;
    }

    public void setZookeeper(String zookeeper) {
        this.zookeeper = zookeeper;
    }

    public String getZkPath() {
        return zkPath;
    }

    public void setZkPath(String zkPath) {
        this.zkPath = zkPath;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBrokers() {
        return brokers;
    }

    public void setBrokers(String brokers) {
        this.brokers = brokers;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getFi() {
        return fi;
    }

    public void setFi(String fi) {
        this.fi = fi;
    }

    public String getFo() {
        return fo;
    }

    public void setFo(String fo) {
        this.fo = fo;
    }

    @Override
    public String toString() {
        return JSONUtils.toJsonString(this);
    }
}
