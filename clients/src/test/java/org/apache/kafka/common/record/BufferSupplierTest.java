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

package org.apache.kafka.common.record;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class BufferSupplierTest {

    @Test
    public void testGrowableBuffer() {
        BufferSupplier.GrowableBufferSupplier supplier = new BufferSupplier.GrowableBufferSupplier();
        ByteBuffer buffer = supplier.get(1024);
        assertEquals(0, buffer.position());
        assertEquals(1024, buffer.capacity());
        supplier.release(buffer);

        ByteBuffer cached = supplier.get(512);
        assertEquals(0, cached.position());
        assertSame(buffer, cached);

        ByteBuffer increased = supplier.get(2048);
        assertEquals(2048, increased.capacity());
        assertEquals(0, increased.position());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSingleton() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        int threadCount = 4;
        ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
        AtomicBoolean closed = new AtomicBoolean(false);
        try {
            IntStream.range(0, threadCount).forEach(index ->
                threadPool.execute(() -> {
                    try {
                        while (!closed.get()) {
                            ByteBuffer buffer = BufferSupplier.SINGLETON.get(index % 2 == 0 ? 10 : 20);
                            BufferSupplier.SINGLETON.release(buffer);
                            TimeUnit.MILLISECONDS.sleep(10);
                        }
                    } catch (InterruptedException e) {
                        // nothing
                    }
                })
            );
            TimeUnit.SECONDS.sleep(5);
        } finally {
            closed.set(true);
            threadPool.shutdown();
            assertTrue(threadPool.awaitTermination(30, TimeUnit.SECONDS));
        }

        ConcurrentMap<Integer, Deque<ByteBuffer>> bufferMap = (ConcurrentMap<Integer, Deque<ByteBuffer>>) BufferSupplier.SINGLETON
                .getClass().getDeclaredField("bufferMap").get(BufferSupplier.SINGLETON);
        assertEquals(2, bufferMap.size());
        bufferMap.values().forEach(queue -> assertEquals(2, queue.size()));

        BufferSupplier.SINGLETON.close();
        assertEquals(0, bufferMap.size());
    }

}
