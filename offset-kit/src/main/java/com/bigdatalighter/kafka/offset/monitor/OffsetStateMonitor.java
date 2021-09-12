package com.bigdatalighter.kafka.offset.monitor;

import java.util.HashSet;
import java.util.Set;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class OffsetStateMonitor {
    private String name;
    private ConsumerOffsetState offsetState;
    private Set<AbsConsumerOffsetStateObserver> observers;
    private Boolean isEnabled;
    private boolean finished;

    public void doMonitor() {
        offsetState.checkState();
        if(offsetState.countObservers() == 0) {
            setFinished(true);
        }
    }

    public void addObserver(AbsConsumerOffsetStateObserver observer) {
        if (observers == null) {
            observers = new HashSet<>();
        }
        observers.add(observer);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ConsumerOffsetState getOffsetState() {
        return offsetState;
    }

    public void setOffsetState(ConsumerOffsetState offsetState) {
        this.offsetState = offsetState;
    }

    public Set<AbsConsumerOffsetStateObserver> getObservers() {
        return observers;
    }

    public void setObservers(Set<AbsConsumerOffsetStateObserver> observers) {
        this.observers = observers;
    }

    public Boolean isEnabled() {
        return isEnabled;
    }

    public void setEnable(Boolean enable) {
        isEnabled = enable;
    }

    public boolean isFinished() {
        return finished;
    }

    public void setFinished(boolean finished) {
        this.finished = finished;
    }
}
