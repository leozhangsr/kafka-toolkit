package com.bigdatalighter.kafka.utils;

import junit.framework.TestCase;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class PathMergerTest extends TestCase {
    private PathMerger pathMerger = new PathMerger();

    public void testPathMerger() {
        String a = "a";
        String b = "b";
        String aWithPrefix = "/a";
        String bWithPrefix = "/b";
        String expect = "/a/b";
        assertEquals(expect, pathMerger.mergePath(a, b));
        assertEquals(expect, pathMerger.mergePath(aWithPrefix, b));
        assertEquals(expect, pathMerger.mergePath(aWithPrefix, bWithPrefix));
        assertEquals(expect, pathMerger.mergePath(a, bWithPrefix));
    }

}
