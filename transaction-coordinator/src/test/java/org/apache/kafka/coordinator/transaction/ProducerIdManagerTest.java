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
package org.apache.kafka.coordinator.transaction;

import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.message.AllocateProducerIdsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AllocateProducerIdsResponse;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.common.ProducerIdsBlock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.coordinator.transaction.RPCProducerIdManager.RETRY_BACKOFF_MS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProducerIdManagerTest {

    private final NodeToControllerChannelManager brokerToController = Mockito.mock(NodeToControllerChannelManager.class);

    // Mutable test implementation that lets us easily set the idStart and error
    class MockProducerIdManager extends RPCProducerIdManager {
        private final Queue<Errors> errorQueue;
        private final boolean isErroneousBlock;
        private final AtomicBoolean capturedFailure = new AtomicBoolean(false);
        private final ExecutorService brokerToControllerRequestExecutor = Executors.newSingleThreadExecutor();
        private final int idLen;
        private Long idStart;

        MockProducerIdManager(int brokerId,
                              long idStart,
                              int idLen,
                              Queue<Errors> errorQueue,
                              boolean isErroneousBlock,
                              Time time) {
            super(brokerId, time, () -> 1L, brokerToController);
            this.idStart = idStart;
            this.idLen = idLen;
            this.errorQueue = errorQueue;
            this.isErroneousBlock = isErroneousBlock;
        }

        @Override
        protected void sendRequest() {
            brokerToControllerRequestExecutor.submit(() -> {
                Errors error = errorQueue.poll();
                if (error == null || error == Errors.NONE) {
                    handleAllocateProducerIdsResponse(new AllocateProducerIdsResponse(
                            new AllocateProducerIdsResponseData()
                                    .setProducerIdStart(idStart)
                                    .setProducerIdLen(idLen)
                    ));
                    if (!isErroneousBlock) {
                        idStart += idLen;
                    }
                } else {
                    handleAllocateProducerIdsResponse(new AllocateProducerIdsResponse(
                            new AllocateProducerIdsResponseData().setErrorCode(error.code())
                    ));
                }
            }, 0);
        }

        @Override
        protected void handleAllocateProducerIdsResponse(AllocateProducerIdsResponse response) {
            super.handleAllocateProducerIdsResponse(response);
            capturedFailure.set(nextProducerIdBlock.get() == null);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    public void testConcurrentGeneratePidRequests(int idBlockLen) throws InterruptedException {
        // Send concurrent generateProducerId requests. Ensure that the generated producer id is unique.
        // For each block (total 3 blocks), only "idBlockLen" number of requests should go through.
        // All other requests should fail immediately.
        var numThreads = 5;
        var latch = new CountDownLatch(idBlockLen * 3);
        var manager = new MockProducerIdManager(0, 0, idBlockLen,
                new ConcurrentLinkedQueue<>(), false, Time.SYSTEM);
        var requestHandlerThreadPool = Executors.newFixedThreadPool(numThreads);
        Map<Long, Integer> pidMap = new ConcurrentHashMap<>();

        for (int i = 0; i < numThreads; i++) {
            requestHandlerThreadPool.submit(() -> {
                while (latch.getCount() > 0) {
                    long result;
                    try {
                        result = manager.generateProducerId();
                        synchronized (pidMap) {
                            if (latch.getCount() != 0) {
                                pidMap.merge(result, 1, Integer::sum);
                                latch.countDown();
                            }
                        }
                    } catch (Exception e) {
                        assertEquals(CoordinatorLoadInProgressException.class, e.getClass());
                    }
                    assertDoesNotThrow(() -> Thread.sleep(100));
                }
            });
        }
        assertTrue(latch.await(12000, TimeUnit.MILLISECONDS));
        requestHandlerThreadPool.shutdown();

        assertEquals(idBlockLen * 3, pidMap.size());
        pidMap.forEach((pid, count) -> {
            assertEquals(1, count);
            assertTrue(pid < (3L * idBlockLen) + numThreads, "Unexpected pid " + pid + "; non-contiguous blocks generated or did not fully exhaust blocks.");
        });
    }

    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"UNKNOWN_SERVER_ERROR", "INVALID_REQUEST"})
    public void testUnrecoverableErrors(Errors error) throws Exception {
        var time = new MockTime();
        var manager = new MockProducerIdManager(0, 0, 1, queue(Errors.NONE, error), false, time);
        verifyNewBlockAndProducerId(manager, new ProducerIdsBlock(0, 0, 1), 0);
        verifyFailureWithoutGenerateProducerId(manager);

        time.sleep(RETRY_BACKOFF_MS);
        verifyNewBlockAndProducerId(manager, new ProducerIdsBlock(0, 1, 1), 1);
    }

    @Test
    public void testInvalidRanges() throws InterruptedException {
        var manager = new MockProducerIdManager(0, -1, 10, new ConcurrentLinkedQueue<>(), true, Time.SYSTEM);
        verifyFailure(manager);

        manager = new MockProducerIdManager(0, 0, -1, new ConcurrentLinkedQueue<>(), true, Time.SYSTEM);
        verifyFailure(manager);

        manager = new MockProducerIdManager(0, Long.MAX_VALUE - 1, 10, new ConcurrentLinkedQueue<>(), true, Time.SYSTEM);
        verifyFailure(manager);
    }

    @Test
    public void testRetryBackoff() throws Exception {
        var time = new MockTime();
        var manager = new MockProducerIdManager(0, 0, 1, queue(Errors.UNKNOWN_SERVER_ERROR), false, time);

        verifyFailure(manager);

        assertThrows(CoordinatorLoadInProgressException.class, manager::generateProducerId);
        time.sleep(RETRY_BACKOFF_MS);
        verifyNewBlockAndProducerId(manager, new ProducerIdsBlock(0, 0, 1), 0);
    }

    private Queue<Errors> queue(Errors... errors) {
        Queue<Errors> queue = new ConcurrentLinkedQueue<>();
        Collections.addAll(queue, errors);
        return queue;
    }

    private void verifyFailure(MockProducerIdManager manager) throws InterruptedException {
        assertThrows(CoordinatorLoadInProgressException.class, manager::generateProducerId);
        verifyFailureWithoutGenerateProducerId(manager);
    }

    private void verifyFailureWithoutGenerateProducerId(MockProducerIdManager manager) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            synchronized (manager) {
                return manager.capturedFailure.get();
            }
        }, "Expected failure");
        manager.capturedFailure.set(false);
    }

    private void verifyNewBlockAndProducerId(MockProducerIdManager manager,
                                             ProducerIdsBlock expectedBlock,
                                             long expectedPid
    ) throws Exception {
        assertThrows(CoordinatorLoadInProgressException.class, manager::generateProducerId);
        TestUtils.waitForCondition(() -> {
            ProducerIdsBlock nextBlock = manager.nextProducerIdBlock.get();
            return nextBlock != null && nextBlock.equals(expectedBlock);
        }, "failed to generate block");
        assertEquals(expectedPid, manager.generateProducerId());
    }
}