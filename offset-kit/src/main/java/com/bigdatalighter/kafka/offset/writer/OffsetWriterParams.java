package com.bigdatalighter.kafka.offset.writer;

import com.bigdatalighter.kafka.utils.JSONUtils;
import org.kohsuke.args4j.Option;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class OffsetWriterParams {
    @Option(name = "--type", aliases = "-t", usage = "Type of OffsetWriter, known type:consumer08,camus,spout,json-file", required = true)
    private String type;
    @Option(name="--zookeeper", aliases = "-zk", usage = "Zookeeper connection in the form host:port")
    private String zookeeper;
    @Option(name="--zk-path", usage = "Namespace path for zookeeper")
    private String zkPath;
    @Option(name = "--brokers", aliases = "-b", usage = "Kafka brokers in the form host:port")
    private String brokers;
    @Option(name = "-fi", usage = "File to read from", required = true)
    private String fi;

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

    public String getBrokers() {
        return brokers;
    }

    public void setBrokers(String brokers) {
        this.brokers = brokers;
    }

    public String getFi() {
        return fi;
    }

    public void setFi(String fi) {
        this.fi = fi;
    }

    @Override
    public String toString() {
        return JSONUtils.toJsonString(this);
    }
}
