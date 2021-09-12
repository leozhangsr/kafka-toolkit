package com.bigdatalighter.kafka.offset.monitor;

import com.bigdatalighter.kafka.offset.reader.OffsetReaderParams;
import com.bigdatalighter.kafka.utils.JSONUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class OffsetStateMonitorManagerConfig {
    private Map<String, OffsetReaderParams> readerConfig;
    private Map<String, OffsetStateObserverParams> observerConfig;
    private Map<String, MonitorConfig> monitorConfig;

    public void addReaderConfig(String name, OffsetReaderParams readerParams) {
        if (readerConfig == null) {
            readerConfig = new HashMap<>();
        }
        readerConfig.put(name, readerParams);
    }

    public void addObserverConfig(String name, OffsetStateObserverParams observerParams) {
        if (observerConfig == null) {
            observerConfig = new HashMap<>();
        }
        observerConfig.put(name, observerParams);
    }

    public void addMonitorConfig(String name, MonitorConfig config) {
        if (monitorConfig == null) {
            monitorConfig = new HashMap<>();
        }
        monitorConfig.put(name, config);
    }

    public Map<String, OffsetReaderParams> getReaderConfig() {
        return readerConfig;
    }

    public void setReaderConfig(Map<String, OffsetReaderParams> readerConfig) {
        this.readerConfig = readerConfig;
    }

    public Map<String, OffsetStateObserverParams> getObserverConfig() {
        return observerConfig;
    }

    public void setObserverConfig(Map<String, OffsetStateObserverParams> observerConfig) {
        this.observerConfig = observerConfig;
    }

    public Map<String, MonitorConfig> getMonitorConfig() {
        return monitorConfig;
    }

    public void setMonitorConfig(Map<String, MonitorConfig> monitorConfig) {
        this.monitorConfig = monitorConfig;
    }

    @Override
    public String toString() {
        return JSONUtils.toJsonString(this);
    }

    public static class MonitorConfig {
        private ConsumerOffsetStateConfig consumerState;
        private Set<String> observers;
        private Boolean isEnabled;

        public ConsumerOffsetStateConfig getConsumerState() {
            return consumerState;
        }

        public void setConsumerState(ConsumerOffsetStateConfig consumerState) {
            this.consumerState = consumerState;
        }

        public Set<String> getObservers() {
            return observers;
        }

        public void setObservers(Set<String> observers) {
            this.observers = observers;
        }

        public Boolean getEnable() {
            return isEnabled;
        }

        public void setEnable(Boolean enable) {
            isEnabled = enable;
        }

        @Override
        public String toString() {
            return JSONUtils.toJsonString(this);
        }
    }

    public static class ConsumerOffsetStateConfig {
        private String targetOffsetReader;
        private String consumerOffsetReader;
        private String topic;
        private String group;

        public String getTargetOffsetReader() {
            return targetOffsetReader;
        }

        public void setTargetOffsetReader(String targetOffsetReader) {
            this.targetOffsetReader = targetOffsetReader;
        }

        public String getConsumerOffsetReader() {
            return consumerOffsetReader;
        }

        public void setConsumerOffsetReader(String consumerOffsetReader) {
            this.consumerOffsetReader = consumerOffsetReader;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        @Override
        public String toString() {
            return JSONUtils.toJsonString(this);
        }
    }
}
