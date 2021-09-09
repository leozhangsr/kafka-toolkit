package com.bigdatalighter.kafka.utils;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class PathMerger {
    public static final String PATH_DELIMITER = "/";
    private StringBuilder pathMerger;

    public PathMerger() {
        pathMerger = new StringBuilder();
    }

    public String mergePath(String... paths) {
        for (int i = 0; i < paths.length && paths[i] != null ; i++) {
            if (paths[i].startsWith(PATH_DELIMITER)) {
                pathMerger.append(paths[i]);
            } else {
                pathMerger.append(PATH_DELIMITER).append(paths[i]);
            }
        }
        String path = pathMerger.toString();
        pathMerger.delete(0, pathMerger.length());
        return path;
    }

}
