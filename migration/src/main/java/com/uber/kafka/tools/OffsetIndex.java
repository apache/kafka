// Copyright (c) 2015 Uber Technologies, Inc. All rights reserved.
// @author Seung-Yeoul Yang (syyang@uber.com)

package com.uber.kafka.tools;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * Represents a list of latest Kafka offsets in increasing order. The latest offsets for every
 * topic are periodically written to a file on every leaf Kafka host by a cron job. The file is
 * encoded as two parallel timestamp and offsets arrays. Timestamps are encoded as 4-bytes
 * little endian unsigned ints, and offsets 8-bytes little endian unsigned longs.
 *
 * See https://code.uberinternal.com/diffusion/SO/browse/master/sortsol/meta_client.py for
 * more details.
 */
public class OffsetIndex {

    public static final long LATEST_OFFSET = -2;

    private static final int SIZE_OF_INT = 4;
    private static final int SIZE_OF_LONG = 8;

    private final List<Long> offsets;

    public OffsetIndex(byte[] encodedOffsets) {
        Preconditions.checkArgument(encodedOffsets.length > 0, "Encoded offsets are empty");
        Preconditions.checkArgument(encodedOffsets.length % 3 == 0, "Invalid encoded offsets");
        final int size = encodedOffsets.length / 3 / SIZE_OF_INT;

        offsets = Lists.newArrayListWithCapacity(size);

        // Skip timestamps since we don't use them.
        int bufOffset = SIZE_OF_INT * size;
        int bufLen = SIZE_OF_LONG * size;
        ByteBuffer buffer = ByteBuffer.wrap(encodedOffsets, bufOffset, bufLen)
            .order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < size; i++) {
            long offset = buffer.getLong();
            offsets.add(offset);
        }
        Collections.sort(offsets);
    }

    public long getNextOffset(long offset) {
        int idx = Collections.binarySearch(offsets, offset);
        if (idx >= 0) {
            return offsets.get(idx + 1);
        }
        int insertionPoint = (idx + 1) * -1;
        return insertionPoint >= offsets.size() ? LATEST_OFFSET : offsets.get(insertionPoint);
    }

    public static OffsetIndex load(String path) {
        try {
            byte[] encodedOffsets = Files.toByteArray(new File(path));
            return new OffsetIndex(encodedOffsets);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read encoded bytes from "  + path, e);
        }
    }
}
