package com.bigdatalighter.kafka;

import com.bigdatalighter.kafka.offset.monitor.OffsetStateMonitorManager;
import com.bigdatalighter.kafka.offset.monitor.OffsetStateMonitorManagerConfig;
import com.bigdatalighter.kafka.utils.JSONUtils;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class MonitorConsumerOffset {

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Monitor config file is required.\r\nUsage: configFile");
            System.exit(0);
        }
        OffsetStateMonitorManagerConfig managerConfig = readManagerConfig(args[0]);
        OffsetStateMonitorManager manager = new OffsetStateMonitorManager(managerConfig);
        while (!manager.isFinished()) {
            manager.doManage();
            if (!manager.isFinished()) {
                try {
                    Thread.sleep(60*1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        manager.close();
    }

    private static OffsetStateMonitorManagerConfig readManagerConfig(String arg) throws IOException {
        File file = new File(arg);
        byte[] bytes = Files.toByteArray(file);
        return JSONUtils.jsonToPojo(new String(bytes), OffsetStateMonitorManagerConfig.class);
    }

}
