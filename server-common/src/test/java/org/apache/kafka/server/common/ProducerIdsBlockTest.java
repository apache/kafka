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
package org.apache.kafka.server.common;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerIdsBlockTest {

    @Test
    public void testEmptyBlock() {
        assertEquals(-1, ProducerIdsBlock.EMPTY.lastProducerId());
        assertEquals(0, ProducerIdsBlock.EMPTY.nextBlockFirstId());
        assertEquals(0, ProducerIdsBlock.EMPTY.size());
    }

    @Test
    public void testDynamicBlock() {
        long firstId = 1309418324L;
        int blockSize = 5391;
        int brokerId = 5;

        ProducerIdsBlock block = new ProducerIdsBlock(brokerId, firstId, blockSize);
        assertEquals(firstId, block.firstProducerId());
        assertEquals(firstId + blockSize - 1, block.lastProducerId());
        assertEquals(firstId + blockSize, block.nextBlockFirstId());
        assertEquals(blockSize, block.size());
        assertEquals(brokerId, block.assignedBrokerId());
    }

    @Test
    public void testClaimNextId() throws Exception {
        for (int i = 0; i < 50; i++) {
            ProducerIdsBlock block = new ProducerIdsBlock(0, 1, 1);
            CountDownLatch latch = new CountDownLatch(1);
            AtomicLong counter = new AtomicLong(0);
            CompletableFuture.runAsync(() -> {
                Optional<Long> pid = block.claimNextId();
                counter.addAndGet(pid.orElse(0L));
                latch.countDown();
            });
            Optional<Long> pid = block.claimNextId();
            counter.addAndGet(pid.orElse(0L));
            assertTrue(latch.await(1, TimeUnit.SECONDS));
            assertEquals(1, counter.get());
        }
    }

}