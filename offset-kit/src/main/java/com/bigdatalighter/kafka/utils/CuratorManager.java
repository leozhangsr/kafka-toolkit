package com.bigdatalighter.kafka.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class CuratorManager {
    private static final int SLEEP_TIME_MS = 3000;
    private static final int MAX_RETRY = 3;
    private static final ThreadLocal<Map<String, CuratorFramework>> curatorCache = new ThreadLocal<>();

    private CuratorManager() {
    }

    public static CuratorFramework directCreate(String zk) {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(SLEEP_TIME_MS, MAX_RETRY);
        return CuratorFrameworkFactory.newClient(zk, retry);
    }

    public static CuratorFramework getCurator(String zk) {
        Map<String, CuratorFramework> map = curatorCache.get();
        if (map == null) {
            map = new HashMap<>();
            curatorCache.set(map);
        }
        CuratorFramework framework = map.get(zk);
        if (framework == null) {
            framework = directCreate(zk);
            map.put(zk, framework);
        }
        return framework;
    }

    public static void destroy() {
        Map<String, CuratorFramework> curatorMap = curatorCache.get();
        if (curatorMap != null) {
            for (CuratorFramework curator : curatorMap.values()) {
                curator.close();
            }
            curatorCache.remove();
        }
    }

}
