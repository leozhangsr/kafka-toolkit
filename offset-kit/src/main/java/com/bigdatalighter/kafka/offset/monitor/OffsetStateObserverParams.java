package com.bigdatalighter.kafka.offset.monitor;

import com.bigdatalighter.kafka.utils.JSONUtils;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class OffsetStateObserverParams {
    private String type;
    private Object params;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Object getParams() {
        return params;
    }

    public void setParams(Object params) {
        this.params = params;
    }

    @Override
    public String toString() {
        return JSONUtils.toJsonString(this);
    }
}
