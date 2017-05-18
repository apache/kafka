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
package org.apache.kafka.clients.producer.internals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
public class MmapBufferPoolTest {
    private final long maxBlockTimeMs = 2000;

    @Test
    public void testSimple() throws Exception {
        long totalMemory = 64 * 1024;
        int size = 1024;
        File storefile = new File(System.getProperty("java.io.tmpdir"), "kafka-producer-data-" + new Random().nextInt(Integer.MAX_VALUE) + ".dat");
        BufferPool pool = new MmapBufferPool(storefile, totalMemory, size);
        ByteBuffer buffer = pool.allocate(size, maxBlockTimeMs);
        assertEquals("Buffer size should equal requested size.", size, buffer.limit());
        assertEquals("Unallocated memory should have shrunk", totalMemory - size, pool.unallocatedMemory());
        assertEquals("Available memory should have shrunk", totalMemory - size, pool.availableMemory());
        buffer.putInt(1);
        buffer.flip();
        pool.deallocate(buffer);
        assertEquals("All memory should be available", totalMemory, pool.availableMemory());
        assertEquals("But now some is on the free list", totalMemory - size, pool.unallocatedMemory());
    }
}
