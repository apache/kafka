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
import java.util.Map;
import org.apache.kafka.common.Configurable;


public class GlobalPoolDelegate implements MemoryPool, Configurable {
    // A specific instance of MemoryPool to be used. Useful in cases where the MemoryPool is supposed to be shared
    // amongst various Kafka Consumers. Please specify pool.class.name as org.apache.kafka.common.memory.GlobalPoolDelegate
    // in order for this to work.
    private static final String POOL_INSTANCE_CONFIG = "linkedin.memorypool.pool.instance";
    private MemoryPool delegateMemoryPool;

    @Override
    public void configure(Map<String, ?> configs) {
        if (!configs.containsKey(POOL_INSTANCE_CONFIG)) {
            throw new IllegalStateException(POOL_INSTANCE_CONFIG + " not found in configs while configuring "
                + "GlobalPoolDelegate object.");
        }
        delegateMemoryPool = (MemoryPool) configs.get(POOL_INSTANCE_CONFIG);
        if (delegateMemoryPool == null) {
            throw new IllegalArgumentException(POOL_INSTANCE_CONFIG + " supplied as null in configs.");
        }
    }

    @Override
    public ByteBuffer tryAllocate(int sizeBytes) {
        return delegateMemoryPool.tryAllocate(sizeBytes);
    }

    @Override
    public void release(ByteBuffer previouslyAllocated) {
        delegateMemoryPool.release(previouslyAllocated);
    }

    @Override
    public long size() {
        return delegateMemoryPool.size();
    }

    @Override
    public long availableMemory() {
        return delegateMemoryPool.availableMemory();
    }

    @Override
    public boolean isOutOfMemory() {
        return delegateMemoryPool.isOutOfMemory();
    }
}
