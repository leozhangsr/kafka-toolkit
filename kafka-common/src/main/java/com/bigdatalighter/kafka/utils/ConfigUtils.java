package com.bigdatalighter.kafka.utils;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import java.util.List;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class ConfigUtils {

    public static List<String> getStringList(Config config, String path) {
        if (config.hasPath(path)) {
            String string = config.getString(path);
            return Splitter.on(",").omitEmptyStrings().trimResults().splitToList(string);
        }
        return Lists.newArrayList();
    }

    public static String getString(Config config, String path, String def) {
        return config.hasPath(path) ? config.getString(path) : def;
    }

    public static Long getLong(Config config, String path, Long def) {
        return config.hasPath(path) ? config.getLong(path) : def;
    }

    public static Integer getInt(Config config, String path, Integer def) {
        return config.hasPath(path) ? config.getInt(path) : def;
    }

    public static boolean getBoolean(Config config, String path, boolean def) {
        return config.hasPath(path) ? config.getBoolean(path) : def;
    }

    public static double getDouble(Config config, String path, double def) {
        return config.hasPath(path) ? config.getDouble(path) : def;
    }

    public static Config getConfig(Config config, String path, Config def) {
        return config.hasPath(path) ? config.getConfig(path) : def;
    }

}
