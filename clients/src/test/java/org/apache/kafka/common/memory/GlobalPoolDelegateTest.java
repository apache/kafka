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

import java.nio.ByteBuffer;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


public class GlobalPoolDelegateTest {
    @Test(expected = IllegalStateException.class)
    public void testMissingPoolInstance() {
        GlobalPoolDelegate globalPoolDelegate = new GlobalPoolDelegate();
        globalPoolDelegate.configure(Collections.emptyMap());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullPoolInstance() {
        GlobalPoolDelegate globalPoolDelegate = new GlobalPoolDelegate();
        globalPoolDelegate.configure(Collections.singletonMap("linkedin.memorypool.pool.instance", null));
    }

    @Test
    public void testDelegateCalls() {
        MemoryPool mockMemoryPool = Mockito.mock(MemoryPool.class);
        Mockito.when(mockMemoryPool.availableMemory()).thenReturn((long) 100);
        Mockito.when(mockMemoryPool.isOutOfMemory()).thenReturn(false);
        Mockito.when(mockMemoryPool.size()).thenReturn((long) 200);
        ByteBuffer byteBuf = ByteBuffer.allocate(0);
        Mockito.when(mockMemoryPool.tryAllocate(Mockito.anyInt())).thenReturn(byteBuf);
        Mockito.doNothing().when(mockMemoryPool).release(Mockito.any());

        GlobalPoolDelegate globalPoolDelegate = new GlobalPoolDelegate();
        globalPoolDelegate.configure(
            Collections.singletonMap("linkedin.memorypool.pool.instance", mockMemoryPool));

        Assert.assertEquals(globalPoolDelegate.availableMemory(), 100);
        Assert.assertEquals(globalPoolDelegate.isOutOfMemory(), false);
        Assert.assertEquals(globalPoolDelegate.size(), 200);
        Assert.assertEquals(globalPoolDelegate.tryAllocate(0), byteBuf);
        globalPoolDelegate.release(byteBuf);

        Mockito.verify(mockMemoryPool, Mockito.times(1)).availableMemory();
        Mockito.verify(mockMemoryPool, Mockito.times(1)).isOutOfMemory();
        Mockito.verify(mockMemoryPool, Mockito.times(1)).size();
        Mockito.verify(mockMemoryPool, Mockito.times(1)).tryAllocate(Mockito.anyInt());
        Mockito.verify(mockMemoryPool, Mockito.times(1)).release(Mockito.any());
    }
}
