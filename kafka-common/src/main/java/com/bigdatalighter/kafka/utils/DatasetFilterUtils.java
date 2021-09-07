package com.bigdatalighter.kafka.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class DatasetFilterUtils {
    public DatasetFilterUtils() {
    }

    public static List<Pattern> getPatternList(Config config, String key) {
        List<Pattern> patterns = Lists.newArrayList();
        if (config.hasPath(key)) {
            List<String> stringList = ConfigUtils.getStringList(config, key);
            for (String str : stringList) {
                patterns.add(Pattern.compile(str));
            }
        }
        return patterns;
    }

    public static List<Pattern> getPatternsFromStrings(List<String> strings) {
        List<Pattern> patterns = Lists.newArrayList();
        Iterator var2 = strings.iterator();

        while(var2.hasNext()) {
            String s = (String)var2.next();
            patterns.add(Pattern.compile(s));
        }

        return patterns;
    }

    public static List<String> filter(List<String> topics, List<Pattern> blacklist, List<Pattern> whitelist) {
        List<String> result = Lists.newArrayList();
        Iterator var4 = topics.iterator();

        while(var4.hasNext()) {
            String topic = (String)var4.next();
            if (survived(topic, blacklist, whitelist)) {
                result.add(topic);
            }
        }

        return result;
    }

    public static Set<String> filter(Set<String> topics, List<Pattern> blacklist, List<Pattern> whitelist) {
        Set<String> result = Sets.newHashSet();
        Iterator var4 = topics.iterator();

        while(var4.hasNext()) {
            String topic = (String)var4.next();
            if (survived(topic, blacklist, whitelist)) {
                result.add(topic);
            }
        }

        return result;
    }

    public static boolean survived(String topic, List<Pattern> blacklist, List<Pattern> whitelist) {
        if (stringInPatterns(topic, blacklist)) {
            return false;
        } else {
            return whitelist.isEmpty() || stringInPatterns(topic, whitelist);
        }
    }

    public static boolean stringInPatterns(String s, List<Pattern> patterns) {
        Iterator var2 = patterns.iterator();

        Pattern pattern;
        do {
            if (!var2.hasNext()) {
                return false;
            }

            pattern = (Pattern)var2.next();
        } while(!pattern.matcher(s).matches());

        return true;
    }
}