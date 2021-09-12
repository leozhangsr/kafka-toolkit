package com.bigdatalighter.kafka.utils;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class Closer {

    public static void close(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
