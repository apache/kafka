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

package org.apache.kafka.common.memory;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class BounceBufferPoolTest {
    private static final Logger log = LoggerFactory.getLogger(BounceBufferPoolTest.class);

    private static class TestBounceBufferFactory implements BounceBufferFactory {
        private final IdentityHashMap<ByteBuffer, Boolean> map = new IdentityHashMap<>();
        private final BounceBufferFactory underlyingFactory;

        TestBounceBufferFactory(BounceBufferFactory underlyingFactory) {
            this.underlyingFactory = underlyingFactory;
        }

        @Override
        public ByteBuffer allocate(int maxSize) {
            ByteBuffer buffer = underlyingFactory.allocate(maxSize);
            Assert.assertFalse(map.containsKey(buffer));
            map.put(buffer, true);
            return buffer;
        }

        @Override
        public void free(ByteBuffer buffer) {
            Assert.assertTrue(map.containsKey(buffer));
            Assert.assertEquals(true, map.get(buffer));
            map.put(buffer, false);
            underlyingFactory.free(buffer);
        }

        int total() {
            return map.size();
        }

        int freed() {
            return (int) map.values().stream().filter(v -> !v).count();
        }
    }

    @Test
    public void testCreateAndClose() throws Exception {
        BounceBufferPool pool = new BounceBufferPool(
            log, new DirectBounceBufferFactory(), 1024, 3);
        pool.close();
    }

    @Test
    public void testAllocateAndDeallocate() throws Exception {
        TestBounceBufferFactory bufferFactory = new TestBounceBufferFactory(
            new DirectBounceBufferFactory());
        BounceBufferPool pool = new BounceBufferPool(log, bufferFactory, 1024, 3);
        Assert.assertEquals(0, pool.curBuffers());
        Assert.assertEquals(0, bufferFactory.total());
        Assert.assertEquals(0, bufferFactory.freed());
        ByteBuffer[] buffers = new ByteBuffer[3];
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = pool.allocate();
            Assert.assertTrue(buffers[i].isDirect());
            Assert.assertEquals(i + 1, pool.curBuffers());
            Assert.assertEquals(i + 1, bufferFactory.total());
            Assert.assertEquals(0, bufferFactory.freed());
        }
        for (int i = 0; i < buffers.length; i++) {
            pool.release(buffers[i]);
            Assert.assertEquals(buffers.length, pool.curBuffers());
            Assert.assertEquals(buffers.length, bufferFactory.total());
            Assert.assertEquals(0, bufferFactory.freed());
        }
        pool.close();
        Assert.assertEquals(bufferFactory.freed(), bufferFactory.total());
        Assert.assertEquals(3, bufferFactory.freed());
    }

    @Test
    public void testAllocateBlocksWhenPoolIsEmpty() throws Exception {
        TestBounceBufferFactory bufferFactory = new TestBounceBufferFactory(
            new DirectBounceBufferFactory());
        final BounceBufferPool pool = new BounceBufferPool(
            log, bufferFactory, 1024, 2);
        Assert.assertEquals(0, pool.curBuffers());
        ByteBuffer[] buffers = new ByteBuffer[2];
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = pool.allocate();
            Assert.assertTrue(buffers[i].isDirect());
            Assert.assertEquals(i + 1, pool.curBuffers());
            Assert.assertEquals(0, bufferFactory.freed());
            Assert.assertEquals(i + 1, bufferFactory.total());
        }
        final CountDownLatch threadStartedLatch = new CountDownLatch(1);
        final CountDownLatch readyToCloseLatch = new CountDownLatch(1);
        final AtomicReference<ByteBuffer> nextBuffer = new AtomicReference<>();
        final AtomicReference<Boolean> hasReleasedBuffer = new AtomicReference<>(false);
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    threadStartedLatch.countDown();
                    nextBuffer.set(pool.allocate());
                    Assert.assertTrue(hasReleasedBuffer.get());
                    readyToCloseLatch.countDown();
                } catch (Exception e) {
                    throw new RuntimeException("Got exception while calling allocate", e);
                }
            }
        };
        thread.start();
        threadStartedLatch.await();
        hasReleasedBuffer.set(true);
        pool.release(buffers[0]);
        Assert.assertEquals(0, bufferFactory.freed());
        Assert.assertEquals(2, bufferFactory.total());
        readyToCloseLatch.await();
        Assert.assertTrue(buffers[0] == nextBuffer.get());
        thread.join();
        pool.release(buffers[0]);
        pool.close();
        Assert.assertEquals(1, bufferFactory.freed());
        pool.release(buffers[1]);
        Assert.assertEquals(2, bufferFactory.freed());
        Assert.assertEquals(2, bufferFactory.total());
        Assert.assertEquals(0, pool.curBuffers());
    }
}
