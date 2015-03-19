// Copyright (c) 2015 Uber Technologies, Inc. All rights reserved.
// @author Seung-Yeoul Yang (syyang@uber.com)

package com.uber.kafka.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.uber.kafka.tools.OffsetIndexTestUtil.Offset;

/**
 * Tests for {@link com.uber.kafka.tools.OffsetIndex}.
 **/
public class OffsetIndexTest {

    private List<Offset> offsets;
    private byte[] offsetsBytes;

    @Before
    public void setUp() {
        offsets = ImmutableList.of(
            new Offset(3, 30L),
            new Offset(4, 40L),
            new Offset(5, 50L),
            new Offset(1, 10L),
            new Offset(2, 20L)
        );
        offsetsBytes = OffsetIndexTestUtil.toByteArray(offsets);
    }

    @Test
    public void testSimple() {
        OffsetIndex index = new OffsetIndex(offsetsBytes);
        assertEquals(10L, index.getNextOffset(5L));
        assertEquals(30L, index.getNextOffset(20L));
        assertEquals(50L, index.getNextOffset(43L));
        assertEquals(OffsetIndex.LATEST_OFFSET, index.getNextOffset(55L));
    }

    @Test
    public void testEmptyOffsetIndex() {
        try {
            OffsetIndex index = new OffsetIndex(new byte[]{});
            fail("Loading empty encoded offsets should fail");
        } catch (IllegalArgumentException e) {
            // Expected.
        }
    }

    @Test
    public void testLoadFromFile() {
        String path = getClass().getResource("/offset_idx").getPath();
        OffsetIndex index = OffsetIndex.load(path);
        assertEquals(9777L, index.getNextOffset(0L));
    }

}
