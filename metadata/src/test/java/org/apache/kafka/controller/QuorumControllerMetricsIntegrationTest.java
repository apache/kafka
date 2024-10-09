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

package org.apache.kafka.controller;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.metrics.QuorumControllerMetrics;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metalog.LocalLogManagerTestEnv;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.controller.QuorumControllerIntegrationTestUtils.createTopics;
import static org.apache.kafka.controller.QuorumControllerIntegrationTestUtils.forceRenounce;
import static org.apache.kafka.controller.QuorumControllerIntegrationTestUtils.pause;
import static org.apache.kafka.controller.QuorumControllerIntegrationTestUtils.registerBrokersAndUnfence;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class QuorumControllerMetricsIntegrationTest {

    static class MockControllerMetrics extends QuorumControllerMetrics {
        final AtomicBoolean closed = new AtomicBoolean(false);

        MockControllerMetrics() {
            super(Optional.empty(), Time.SYSTEM);
        }

        @Override
        public void close() {
            super.close();
            closed.set(true);
        }
    }

    /**
     * Test that closing the QuorumController closes the metrics object.
     */
    @Test
    public void testClosingQuorumControllerClosesMetrics() throws Throwable {
        MockControllerMetrics metrics = new MockControllerMetrics();
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(1).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                setControllerBuilderInitializer(controllerBuilder ->
                    controllerBuilder.setMetrics(metrics)
                ).
                build()
        ) {
            assertEquals(1, controlEnv.activeController().controllerMetrics().newActiveControllers());
        }
        assertTrue(metrics.closed.get(), "metrics were not closed");
    }

    /**
     * Test that failing over to a new controller increments NewActiveControllersCount on both the
     * active and inactive controllers.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testFailingOverIncrementsNewActiveControllerCount(
        boolean forceFailoverUsingLogLayer
    ) throws Throwable {
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(3).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                build()
        ) {
            registerBrokersAndUnfence(controlEnv.activeController(), 1); // wait for a controller to become active.
            TestUtils.retryOnExceptionWithTimeout(30_000, () -> {
                for (QuorumController controller : controlEnv.controllers()) {
                    assertEquals(1, controller.controllerMetrics().newActiveControllers());
                }
            });
            if (forceFailoverUsingLogLayer) {
                logEnv.activeLogManager().get().throwOnNextAppend();

                TestUtils.retryOnExceptionWithTimeout(30_000, () ->
                    createTopics(controlEnv.activeController(), "test_", 1, 1)
                );
            } else {
                // Directly call QuorumController.renounce.
                forceRenounce(controlEnv.activeController());
            }
            TestUtils.retryOnExceptionWithTimeout(30_000, () -> {
                for (QuorumController controller : controlEnv.controllers()) {
                    assertEquals(2, controller.controllerMetrics().newActiveControllers());
                }
            });
        }
    }

    /**
     * Test the heartbeat and general operation timeout metrics.
     * These are incremented on the active controller only.
     */
    @Test
    public void testTimeoutMetrics() throws Throwable {
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(3).
                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                build()
        ) {
            QuorumController active = controlEnv.activeController();
            Map<Integer, Long> brokerEpochs = registerBrokersAndUnfence(active, 3);
            assertEquals(0L, active.controllerMetrics().timedOutHeartbeats());
            assertEquals(0L, active.controllerMetrics().operationsTimedOut());

            // We pause the controller so that the heartbeat event will definitely be expired
            // rather than processed.
            CountDownLatch latch = pause(active);
            ControllerRequestContext expiredTimeoutContext = new ControllerRequestContext(
                new RequestHeaderData(),
                KafkaPrincipal.ANONYMOUS,
                OptionalLong.of(active.time().nanoseconds()));
            CompletableFuture<BrokerHeartbeatReply> replyFuture =
                active.processBrokerHeartbeat(expiredTimeoutContext,
                    new BrokerHeartbeatRequestData()
                        .setWantFence(false)
                        .setBrokerEpoch(brokerEpochs.get(0))
                        .setBrokerId(0)
                        .setCurrentMetadataOffset(100000));
            latch.countDown(); // Unpause the controller.
            assertEquals(TimeoutException.class,
                assertThrows(ExecutionException.class, replyFuture::get).
                    getCause().getClass());
            assertEquals(1L, active.controllerMetrics().timedOutHeartbeats());
            assertEquals(1L, active.controllerMetrics().operationsTimedOut());

            // Inject a new timed out operation.
            CountDownLatch latch2 = pause(active);
            active.appendControlEventWithDeadline("fakeTimeoutOperation",
                () -> { },
                active.time().nanoseconds());
            latch2.countDown();
            TestUtils.retryOnExceptionWithTimeout(30_000, () -> {
                // The fake timeout increments operationsTimedOut but not timedOutHeartbeats.
                assertEquals(1L, active.controllerMetrics().timedOutHeartbeats());
                assertEquals(2L, active.controllerMetrics().operationsTimedOut());
            });
            for (QuorumController controller : controlEnv.controllers()) {
                // Inactive controllers don't set these metrics.
                if (!controller.isActive()) {
                    assertFalse(controller.controllerMetrics().active());
                    assertEquals(0L, controller.controllerMetrics().timedOutHeartbeats());
                    assertEquals(0L, controller.controllerMetrics().operationsTimedOut());
                }
            }
        }
    }

    /**
     * Test the event queue operations started metric.
     */
    @Test
    public void testEventQueueOperationsStartedMetric() throws Throwable {
        try (
            LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv.Builder(3).
                                                build();
            QuorumControllerTestEnv controlEnv = new QuorumControllerTestEnv.Builder(logEnv).
                                                     build()
        ) {
            QuorumController active = controlEnv.activeController();
            Map<Integer, Long> brokerEpochs = registerBrokersAndUnfence(active, 3);

            // Test that a new operation increments operationsStarted. We retry this if needed
            // to handle the case where another operation is performed in between loading
            // expectedOperationsStarted and running the new control event.
            TestUtils.retryOnExceptionWithTimeout(30_000, () -> {
                long expectedOperationsStarted = active.controllerMetrics().operationsStarted() + 1;
                CompletableFuture<Long> actualOperationsStarted = new CompletableFuture<>();
                active.appendControlEvent("checkOperationsStarted", () ->
                    actualOperationsStarted.complete(active.controllerMetrics().operationsStarted())
                );
                assertEquals(expectedOperationsStarted, actualOperationsStarted.get());
            });
        }
    }
}
