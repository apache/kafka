// Copyright (c) 2015 Uber Technologies, Inc. All rights reserved.
// @author Seung-Yeoul Yang (syyang@uber.com)

package com.uber.kafka.tools;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class OffsetIndexTestUtil {

    private static final int SIZE_OF_INT = 4;
    private static final int SIZE_OF_LONG = 8;

    public static class Offset {
        public final int ts;
        public final long offset;

        public Offset(int ts, long offset) {
            this.ts = ts;
            this.offset = offset;
        }
    }

    public static byte[] toByteArray(List<Offset> offsets) {
        int bufLen = offsets.size() * (SIZE_OF_INT + SIZE_OF_LONG);
        byte[] encodedBytes = new byte[bufLen];
        ByteBuffer buf = ByteBuffer.wrap(encodedBytes).order(ByteOrder.LITTLE_ENDIAN);
        for (Offset offset : offsets) {
            buf.putInt(offset.ts);
        }
        for (Offset offset : offsets) {
            buf.putLong(offset.offset);
        }
        return encodedBytes;
    }

    private OffsetIndexTestUtil() {}
}
