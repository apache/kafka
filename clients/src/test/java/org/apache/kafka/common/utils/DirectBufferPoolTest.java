/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DirectBufferPoolTest {

    private DirectBufferPool pool;

    @AfterEach
    public void tearDown() {
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    public void testBasics() {
        pool = new DirectBufferPool(true);

        ByteBuffer a = pool.allocate(100);
        assertEquals(100, a.capacity());
        assertEquals(100, a.remaining());
        pool.release(a);

        ByteBuffer b = pool.allocate(100);
        assertSame(a, b);

        ByteBuffer c = pool.allocate(100);
        assertNotSame(b, c);
        pool.release(b);
        pool.release(c);
    }

    @Test
    public void testBuffersAreCleared() {
        pool = new DirectBufferPool(true);

        ByteBuffer a = pool.allocate(100);
        a.putInt(0xdeadbeef);
        assertEquals(96, a.remaining());
        pool.release(a);

        ByteBuffer b = pool.allocate(100);
        assertSame(a, b);
        assertEquals(100, a.remaining());
        pool.release(b);
    }

    @Test
    public void testWeakRefClearing() {
        pool = new DirectBufferPool(true);

        List<ByteBuffer> bufs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ByteBuffer buf = pool.allocate(100);
            bufs.add(buf);
        }

        for (ByteBuffer buf : bufs) {
            pool.release(buf);
        }

        assertEquals(10, pool.countBuffersOfSize(100));

        bufs.clear();
        bufs = null;
        for (int i = 0; i < 3; i++) {
            System.gc();
        }

        ByteBuffer buf = pool.allocate(100);
        assertEquals(0, pool.countBuffersOfSize(100));
        pool.release(buf);
    }

    @Test
    public void testPoolingDisabled() {
        pool = new DirectBufferPool(false);

        ByteBuffer a = pool.allocate(100);
        pool.release(a);

        ByteBuffer b = pool.allocate(100);
        assertNotSame(a, b);
    }

    @Test
    public void testInvalidSize() {
        pool = new DirectBufferPool(false);
        assertThrows(IllegalArgumentException.class, () -> pool.allocate(0));
        assertThrows(IllegalArgumentException.class, () -> pool.allocate(-1));
    }

    @Test
    public void testAllocationFailure() {
        pool = new DirectBufferPool(false);

        ByteBuffer buf = pool.allocate(1024);
        assertTrue(buf.isDirect());
        assertEquals(0, pool.directAllocFailures());
    }

    @Test
    public void testAutoReleaseOnGC() throws Exception {
        pool = new DirectBufferPool(true);

        ByteBuffer buffer = pool.allocate(256);
        Object ref = new Object();
        pool.registerForAutoRelease(ref, buffer);

        assertEquals(1, pool.allocations());
        assertEquals(0, pool.autoReleases());

        ref = null;
        for (int i = 0; i < 10 && pool.autoReleases() == 0; i++) {
            System.gc();
            Thread.sleep(50);
        }

        assertEquals(1, pool.autoReleases());

        ByteBuffer reused = pool.allocate(256);
        assertEquals(1, pool.poolHits());
        assertSame(buffer, reused);
    }

    @Test
    public void testAutoReleaseNullHandling() {
        pool = new DirectBufferPool(true);

        pool.registerForAutoRelease(null, pool.allocate(100));
        pool.registerForAutoRelease(new Object(), null);

        DirectBufferPool disabledPool = new DirectBufferPool(false);
        disabledPool.registerForAutoRelease(new Object(), disabledPool.allocate(100));
        assertEquals(0, disabledPool.autoReleases());
    }
}
