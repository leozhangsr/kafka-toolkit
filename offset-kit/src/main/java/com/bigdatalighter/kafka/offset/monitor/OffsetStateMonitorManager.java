package com.bigdatalighter.kafka.offset.monitor;

import com.bigdatalighter.kafka.offset.reader.IOffsetReader;
import com.bigdatalighter.kafka.offset.reader.OffsetReaderFactory;
import com.bigdatalighter.kafka.offset.reader.OffsetReaderParams;
import com.bigdatalighter.kafka.utils.Closer;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class OffsetStateMonitorManager {
    private Logger logger = LoggerFactory.getLogger(OffsetStateMonitorManager.class);
    private Map<String, OffsetStateMonitor> monitors;
    private Map<String, IOffsetReader> nameOffsetReaderMap;
    private Map<String, AbsConsumerOffsetStateObserver> nameObserverMap;

    public OffsetStateMonitorManager(OffsetStateMonitorManagerConfig config) {
        initOffsetReader(config.getReaderConfig());
        initObservers(config.getObserverConfig());
        initMonitors(config.getMonitorConfig());
    }

    public void doManage() {
        if (monitors != null) {
            logger.info("{} monitor is still running", monitors.size());
            Iterator<Map.Entry<String, OffsetStateMonitor>> monitorIterator = monitors.entrySet().iterator();
            while (monitorIterator.hasNext()) {
                Map.Entry<String, OffsetStateMonitor> monitorEntry = monitorIterator.next();
                OffsetStateMonitor monitor = monitorEntry.getValue();
                logger.info("Do monitor for {}", monitor.getName());
                monitor.doMonitor();
                if (monitor.isFinished()) {
                    monitorIterator.remove();
                    logger.info("Monitor[] finished monitor,and was removed", monitorEntry.getKey());
                }
            }
        }
    }

    public boolean isFinished() {
        return monitors.size() == 0;
    }

    public void close() {
        for(IOffsetReader offsetReader : nameOffsetReaderMap.values()) {
            Closer.close(offsetReader);
        }
    }

    private void initOffsetReader(Map<String, OffsetReaderParams> readerConfig) {
        nameOffsetReaderMap = new HashMap<>(readerConfig.size());
        for (Map.Entry<String, OffsetReaderParams> entry : readerConfig.entrySet()) {
            logger.info("Creating OffsetReader - Name:{}, params:{}", entry.getKey(), entry.getValue());
            nameOffsetReaderMap.put(entry.getKey(), OffsetReaderFactory.create(entry.getValue()));
        }
    }

    private void initObservers(Map<String, OffsetStateObserverParams> observerConfig) {
        nameObserverMap = new HashMap<>();
        for (Map.Entry<String, OffsetStateObserverParams> entry : observerConfig.entrySet()) {
            logger.info("Creating AbsConsumerOffsetStateObserver - Name:{}, params:{}", entry.getKey(), entry.getValue());
            nameObserverMap.put(entry.getKey(), OffsetStateObserverFactory.create(entry.getValue()));
        }
    }

    private void initMonitors(Map<String, OffsetStateMonitorManagerConfig.MonitorConfig> monitorConfig) {
        monitors = new HashMap<>();
        for (Map.Entry<String, OffsetStateMonitorManagerConfig.MonitorConfig> entry : monitorConfig.entrySet()) {
            OffsetStateMonitorManagerConfig.MonitorConfig config = entry.getValue();
            if (config.getEnable()) {
                OffsetStateMonitor monitor = new OffsetStateMonitor();
                logger.info("Creating monitor, name:{}, params:{}", entry.getKey(), config);
                ConsumerOffsetState offsetState = createOffsetState(config.getConsumerState());
                for (String observer : config.getObservers()) {
                    AbsConsumerOffsetStateObserver stateObserver = nameObserverMap.get(observer);
                    Preconditions.checkNotNull(stateObserver, "Observer doesn't exists:" + observer);
                    offsetState.addObserver(stateObserver);
                    monitor.addObserver(stateObserver);
                }
                monitor.setOffsetState(offsetState);
                monitor.setName(entry.getKey());
                monitors.put(monitor.getName(), monitor);
            }
        }
    }

    private ConsumerOffsetState createOffsetState(OffsetStateMonitorManagerConfig.ConsumerOffsetStateConfig config) {
        IOffsetReader targetReader = nameOffsetReaderMap.get(config.getTargetOffsetReader());
        IOffsetReader consumerReader = nameOffsetReaderMap.get(config.getConsumerOffsetReader());
        Preconditions.checkNotNull(targetReader, "Target OffsetReader doesn't exists:" + config.getTargetOffsetReader());
        Preconditions.checkNotNull(consumerReader, "Consumer OffsetReader doesn't exists:" + config.getConsumerOffsetReader());
        Preconditions.checkNotNull(config.getTopic(), "Monitor topic can't be null");
        return new ConsumerOffsetState(targetReader, consumerReader, config.getGroup(), config.getTopic());
    }

}
