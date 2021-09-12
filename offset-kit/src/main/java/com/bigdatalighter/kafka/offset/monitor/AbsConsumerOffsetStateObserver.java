package com.bigdatalighter.kafka.offset.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Observable;
import java.util.Observer;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public abstract class AbsConsumerOffsetStateObserver implements Observer {
    Logger logger = LoggerFactory.getLogger(AbsConsumerOffsetStateObserver.class);
    private String name;
    protected boolean isFinish;

    @Override
    public void update(Observable observable, Object changed) {
        ConsumerOffsetState offsetState = (ConsumerOffsetState) observable;
        ConsumerOffsetState.StateInfo stateInfo = (ConsumerOffsetState.StateInfo) changed;
        if (stateInfo.hasCaughtUp()) {
            doCaughtUp(offsetState, stateInfo);
        } else {
            doDelay(offsetState, stateInfo);
        }
    }

    protected abstract void doCaughtUp(ConsumerOffsetState offsetState, ConsumerOffsetState.StateInfo stateInfo);

    protected abstract void doDelay(ConsumerOffsetState offsetState, ConsumerOffsetState.StateInfo stateInfo);

    public void finished() {
        setFinish(true);
    }


    public boolean isFinish() {
        return isFinish;
    }

    public void setFinish(boolean finish) {
        isFinish = finish;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
