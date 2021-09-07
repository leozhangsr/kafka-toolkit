package com.bigdatalighter.kafka.utils;

import java.nio.ByteBuffer;

/**
 * @author: Leo Zhang(johnson5211.work@gmail.com)
 **/
public class ByteBufferUtils {

    public static byte[] getBytes(ByteBuffer buf) {
        byte[] bytes = null;
        if (buf != null) {
            int size = buf.remaining();
            bytes = new byte[size];
            buf.get(bytes, buf.position(), size);
        }
        return bytes;
    }

}
