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
package org.apache.kafka.trogdor.workload;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ShareConsumeBenchWorkerTest {

    private static final ConfigResource GROUP = new ConfigResource(ConfigResource.Type.GROUP, "group");
    private static final String NAME = "errors.deadletterqueue.topic.name";
    private static final String VALUE = "dlq.topic";
    private static final List<AlterConfigOp> OPS = List.of(
        new AlterConfigOp(new ConfigEntry(NAME, VALUE), AlterConfigOp.OpType.SET));
    // The backoff between attempts is 2000 ms plus a random 0 to 100 ms, and there are 3 attempts.
    private static final long MIN_BACKOFF_MS = 2000;
    private static final long MAX_BACKOFF_MS = 2100;
    private static final int ATTEMPTS = 3;

    private Admin admin;
    private MockTime time;

    @BeforeEach
    public void setUp() {
        admin = mock(Admin.class);
        time = new MockTime();
    }

    private void describeReturns(Config config) {
        describeReturns(KafkaFuture.completedFuture(Map.of(GROUP, config)));
    }

    private void describeReturns(KafkaFuture<Map<ConfigResource, Config>> future) {
        DescribeConfigsResult result = mock(DescribeConfigsResult.class);
        when(result.all()).thenReturn(future);
        when(admin.describeConfigs(anyCollection())).thenReturn(result);
    }

    private void describeFails() {
        KafkaFutureImpl<Map<ConfigResource, Config>> failed = new KafkaFutureImpl<>();
        failed.completeExceptionally(new UnknownServerException("describe failed"));
        describeReturns(failed);
    }

    private static AlterConfigsResult alterFailsWith(Throwable error) {
        KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        future.completeExceptionally(error);
        AlterConfigsResult result = mock(AlterConfigsResult.class);
        when(result.all()).thenReturn(future);
        return result;
    }

    private static AlterConfigsResult alterResult(boolean fails) {
        KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        if (fails) {
            future.completeExceptionally(new UnknownServerException("alter failed"));
        } else {
            future.complete(null);
        }
        AlterConfigsResult result = mock(AlterConfigsResult.class);
        when(result.all()).thenReturn(future);
        return result;
    }

    private static Config configWith(String value) {
        return new Config(List.of(new ConfigEntry(NAME, value)));
    }

    private long elapsedMs(long startMs) {
        return time.milliseconds() - startMs;
    }

    @Test
    public void testConfigsAlreadySetAreLeftAlone() throws Exception {
        describeReturns(configWith(VALUE));
        long startMs = time.milliseconds();

        ShareConsumeBenchWorker.alterConfigs(admin, GROUP, OPS, time);

        verify(admin, never()).incrementalAlterConfigs(anyMap());
        assertEquals(0, elapsedMs(startMs));
    }

    @Test
    public void testConfigsWithADifferentValueAreAltered() throws Exception {
        describeReturns(configWith("another.topic"));
        AlterConfigsResult succeeds = alterResult(false);
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(succeeds);
        long startMs = time.milliseconds();

        ShareConsumeBenchWorker.alterConfigs(admin, GROUP, OPS, time);

        verify(admin, times(1)).incrementalAlterConfigs(Map.of(GROUP, OPS));
        assertEquals(0, elapsedMs(startMs));
    }

    @Test
    public void testMissingConfigsAreAltered() throws Exception {
        describeReturns(new Config(List.of()));
        AlterConfigsResult succeeds = alterResult(false);
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(succeeds);

        ShareConsumeBenchWorker.alterConfigs(admin, GROUP, OPS, time);

        verify(admin, times(1)).incrementalAlterConfigs(Map.of(GROUP, OPS));
    }

    @Test
    public void testConfigsAreAlteredWhenTheyCannotBeDescribed() throws Exception {
        describeFails();
        AlterConfigsResult succeeds = alterResult(false);
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(succeeds);
        long startMs = time.milliseconds();

        ShareConsumeBenchWorker.alterConfigs(admin, GROUP, OPS, time);

        verify(admin, times(1)).incrementalAlterConfigs(Map.of(GROUP, OPS));
        assertEquals(0, elapsedMs(startMs));
    }

    @Test
    public void testFailedAlterIsRetriedAfterTheBackoff() throws Exception {
        describeReturns(new Config(List.of()));
        AlterConfigsResult fails = alterResult(true);
        AlterConfigsResult succeeds = alterResult(false);
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(fails, succeeds);
        long startMs = time.milliseconds();

        ShareConsumeBenchWorker.alterConfigs(admin, GROUP, OPS, time);

        verify(admin, times(2)).incrementalAlterConfigs(Map.of(GROUP, OPS));
        long elapsedMs = elapsedMs(startMs);
        assertTrue(elapsedMs >= MIN_BACKOFF_MS && elapsedMs <= MAX_BACKOFF_MS,
            "Expected one backoff of 2000 to 2100 ms, got " + elapsedMs);
    }

    @Test
    public void testRetryStopsWhenAnotherConsumerSetsTheConfigs() throws Exception {
        // The first attempt fails, and by the second the configs have been set by another consumer.
        DescribeConfigsResult unset = mock(DescribeConfigsResult.class);
        when(unset.all()).thenReturn(KafkaFuture.completedFuture(Map.of(GROUP, new Config(List.of()))));
        DescribeConfigsResult set = mock(DescribeConfigsResult.class);
        when(set.all()).thenReturn(KafkaFuture.completedFuture(Map.of(GROUP, configWith(VALUE))));
        when(admin.describeConfigs(anyCollection())).thenReturn(unset, set);
        AlterConfigsResult fails = alterResult(true);
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(fails);

        ShareConsumeBenchWorker.alterConfigs(admin, GROUP, OPS, time);

        verify(admin, times(1)).incrementalAlterConfigs(Map.of(GROUP, OPS));
        verify(admin, times(2)).describeConfigs(anyCollection());
    }

    @Test
    public void testGivesUpAfterAllAttempts() {
        describeReturns(new Config(List.of()));
        AlterConfigsResult fails = alterResult(true);
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(fails);
        long startMs = time.milliseconds();

        RuntimeException e = assertThrows(RuntimeException.class,
            () -> ShareConsumeBenchWorker.alterConfigs(admin, GROUP, OPS, time));

        verify(admin, times(ATTEMPTS)).incrementalAlterConfigs(Map.of(GROUP, OPS));
        assertTrue(e.getMessage().contains("after " + ATTEMPTS + " attempts"), e.getMessage());
        assertInstanceOf(ExecutionException.class, e.getCause());
        // There is a backoff between the attempts, not after the last one.
        long elapsedMs = elapsedMs(startMs);
        assertTrue(elapsedMs >= (ATTEMPTS - 1) * MIN_BACKOFF_MS && elapsedMs <= (ATTEMPTS - 1) * MAX_BACKOFF_MS,
            "Expected 2 backoffs of 2000 to 2100 ms each, got " + elapsedMs);
    }

    @Test
    public void testInterruptIsRestoredAndNotRetried() {
        describeReturns(new Config(List.of()));
        KafkaFuture<Void> interrupted = new KafkaFutureImpl<Void>() {
            @Override
            public Void get(long timeout, TimeUnit unit) throws InterruptedException {
                throw new InterruptedException();
            }
        };
        AlterConfigsResult result = mock(AlterConfigsResult.class);
        when(result.all()).thenReturn(interrupted);
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(result);
        long startMs = time.milliseconds();

        try {
            RuntimeException e = assertThrows(RuntimeException.class,
                () -> ShareConsumeBenchWorker.alterConfigs(admin, GROUP, OPS, time));
            assertInstanceOf(InterruptedException.class, e.getCause());
            assertTrue(Thread.currentThread().isInterrupted());
            verify(admin, times(1)).incrementalAlterConfigs(Map.of(GROUP, OPS));
            assertEquals(0, elapsedMs(startMs));
        } finally {
            // Clear the flag so that it does not leak into other tests.
            Thread.interrupted();
        }
    }

    @Test
    public void testNonRetriableErrorFailsWithoutRetrying() {
        describeReturns(new Config(List.of()));
        KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        future.completeExceptionally(new InvalidConfigurationException("bad config"));
        AlterConfigsResult result = mock(AlterConfigsResult.class);
        when(result.all()).thenReturn(future);
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(result);
        long startMs = time.milliseconds();

        RuntimeException e = assertThrows(RuntimeException.class,
            () -> ShareConsumeBenchWorker.alterConfigs(admin, GROUP, OPS, time));

        assertInstanceOf(ExecutionException.class, e.getCause());
        assertInstanceOf(InvalidConfigurationException.class, e.getCause().getCause());
        verify(admin, times(1)).incrementalAlterConfigs(Map.of(GROUP, OPS));
        assertEquals(0, elapsedMs(startMs));
    }

    @Test
    public void testAuthorizationErrorFailsWithoutRetrying() {
        describeReturns(new Config(List.of()));
        AlterConfigsResult fails = alterFailsWith(new GroupAuthorizationException("not authorized"));
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(fails);
        long startMs = time.milliseconds();

        assertThrows(RuntimeException.class, () -> ShareConsumeBenchWorker.alterConfigs(admin, GROUP, OPS, time));

        verify(admin, times(1)).incrementalAlterConfigs(Map.of(GROUP, OPS));
        assertEquals(0, elapsedMs(startMs));
    }

    @Test
    public void testRetriableApiExceptionIsRetried() throws Exception {
        describeReturns(new Config(List.of()));
        AlterConfigsResult fails = alterFailsWith(new TimeoutException("timed out"));
        AlterConfigsResult succeeds = alterResult(false);
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(fails, succeeds);

        ShareConsumeBenchWorker.alterConfigs(admin, GROUP, OPS, time);

        verify(admin, times(2)).incrementalAlterConfigs(Map.of(GROUP, OPS));
    }

    @Test
    public void testNonApiExceptionIsRetried() throws Exception {
        describeReturns(new Config(List.of()));
        AlterConfigsResult fails = alterFailsWith(new IllegalStateException("client failure"));
        AlterConfigsResult succeeds = alterResult(false);
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(fails, succeeds);

        ShareConsumeBenchWorker.alterConfigs(admin, GROUP, OPS, time);

        verify(admin, times(2)).incrementalAlterConfigs(Map.of(GROUP, OPS));
    }

    @Test
    public void testInterruptDuringDescribeIsRestoredAndNotRetried() {
        KafkaFuture<Map<ConfigResource, Config>> interrupted = new KafkaFutureImpl<Map<ConfigResource, Config>>() {
            @Override
            public Map<ConfigResource, Config> get(long timeout, TimeUnit unit) throws InterruptedException {
                throw new InterruptedException();
            }
        };
        describeReturns(interrupted);
        long startMs = time.milliseconds();

        try {
            RuntimeException e = assertThrows(RuntimeException.class,
                () -> ShareConsumeBenchWorker.alterConfigs(admin, GROUP, OPS, time));
            assertInstanceOf(InterruptedException.class, e.getCause());
            assertTrue(Thread.currentThread().isInterrupted());
            verify(admin, never()).incrementalAlterConfigs(anyMap());
            assertEquals(0, elapsedMs(startMs));
        } finally {
            // Clear the flag so that it does not leak into other tests.
            Thread.interrupted();
        }
    }
}
