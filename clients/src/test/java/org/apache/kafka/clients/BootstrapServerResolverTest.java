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
package org.apache.kafka.clients;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.BootstrapResolutionException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BootstrapServerResolverTest {
    private static final List<String> BOOTSTRAP_SERVERS = List.of("bootstrap.example.com:9092");
    private static final List<InetSocketAddress> RESOLVED_ADDRESSES = List.of(
        new InetSocketAddress("127.0.0.1", 9092),
        new InetSocketAddress("127.0.0.2", 9092));

    @Test
    public void testSuccessfulResolutionBootstrapsMetadata() throws Exception {
        MockTime time = new MockTime();
        MetadataUpdater metadataUpdater = metadataUpdater();
        AtomicBoolean bootstrapped = new AtomicBoolean();
        when(metadataUpdater.isBootstrapped()).thenAnswer(__ -> bootstrapped.get());
        doAnswer(invocation -> {
            bootstrapped.set(true);
            return null;
        }).when(metadataUpdater).bootstrap(RESOLVED_ADDRESSES);

        try (BootstrapServerResolver resolver = newResolver(time, () -> RESOLVED_ADDRESSES)) {
            TestUtils.waitForCondition(() -> {
                resolver.ensureBootstrapped(time.milliseconds(), metadataUpdater);
                return bootstrapped.get();
            }, "Bootstrap resolution did not complete");
        }

        verify(metadataUpdater).bootstrap(RESOLVED_ADDRESSES);
    }

    @Test
    public void testDisabledConfigurationDoesNotResolveOrBootstrap() {
        MockTime time = new MockTime();
        MetadataUpdater metadataUpdater = metadataUpdater();
        AtomicBoolean resolved = new AtomicBoolean();

        try (BootstrapServerResolver resolver = new BootstrapServerResolver(
            BootstrapConfiguration.DISABLED,
            time,
            new LogContext(),
            () -> {
                resolved.set(true);
                return RESOLVED_ADDRESSES;
            })) {
            resolver.ensureBootstrapped(time.milliseconds(), metadataUpdater);
        }

        assertFalse(resolved.get());
        verify(metadataUpdater, never()).bootstrap(any());
        verify(metadataUpdater, never()).bootstrapFailed(any());
    }

    @Test
    public void testResolutionRetriesAfterAnEmptyResult() throws Exception {
        MockTime time = new MockTime();
        MetadataUpdater metadataUpdater = metadataUpdater();
        AtomicBoolean bootstrapped = new AtomicBoolean();
        AtomicInteger attempts = new AtomicInteger();
        when(metadataUpdater.isBootstrapped()).thenAnswer(__ -> bootstrapped.get());
        doAnswer(invocation -> {
            bootstrapped.set(true);
            return null;
        }).when(metadataUpdater).bootstrap(RESOLVED_ADDRESSES);

        BootstrapConfiguration configuration = configuration(1000, 10);
        try (BootstrapServerResolver resolver = new BootstrapServerResolver(
            configuration,
            time,
            new LogContext(),
            () -> attempts.incrementAndGet() == 1 ? List.of() : RESOLVED_ADDRESSES)) {
            TestUtils.waitForCondition(() -> {
                resolver.ensureBootstrapped(time.milliseconds(), metadataUpdater);
                return attempts.get() == 1;
            }, "Initial bootstrap resolution did not complete");

            time.sleep(configuration.retryBackoffMs);
            TestUtils.waitForCondition(() -> {
                resolver.ensureBootstrapped(time.milliseconds(), metadataUpdater);
                return bootstrapped.get();
            }, "Bootstrap resolution did not retry successfully");
        }

        assertEquals(2, attempts.get());
        verify(metadataUpdater).bootstrap(RESOLVED_ADDRESSES);
    }

    @Test
    public void testResolutionRetriesAfterAnException() throws Exception {
        MockTime time = new MockTime();
        MetadataUpdater metadataUpdater = metadataUpdater();
        AtomicBoolean bootstrapped = new AtomicBoolean();
        AtomicInteger attempts = new AtomicInteger();
        when(metadataUpdater.isBootstrapped()).thenAnswer(__ -> bootstrapped.get());
        doAnswer(invocation -> {
            bootstrapped.set(true);
            return null;
        }).when(metadataUpdater).bootstrap(RESOLVED_ADDRESSES);

        BootstrapConfiguration configuration = configuration(1000, 10);
        try (BootstrapServerResolver resolver = new BootstrapServerResolver(
            configuration,
            time,
            new LogContext(),
            () -> {
                if (attempts.incrementAndGet() == 1)
                    throw new IllegalStateException("DNS lookup failed");
                return RESOLVED_ADDRESSES;
            })) {
            TestUtils.waitForCondition(() -> {
                resolver.ensureBootstrapped(time.milliseconds(), metadataUpdater);
                return attempts.get() == 1;
            }, "Initial bootstrap resolution did not complete");

            time.sleep(configuration.retryBackoffMs);
            TestUtils.waitForCondition(() -> {
                resolver.ensureBootstrapped(time.milliseconds(), metadataUpdater);
                return bootstrapped.get();
            }, "Bootstrap resolution did not retry successfully");
        }

        assertEquals(2, attempts.get());
        verify(metadataUpdater).bootstrap(RESOLVED_ADDRESSES);
    }

    @Test
    public void testResolutionFailureIsReportedAfterTimeout() throws Exception {
        MockTime time = new MockTime();
        MetadataUpdater metadataUpdater = metadataUpdater();
        BootstrapConfiguration configuration = configuration(100, 10);
        AtomicInteger attempts = new AtomicInteger();

        try (BootstrapServerResolver resolver = new BootstrapServerResolver(
            configuration,
            time,
            new LogContext(),
            () -> {
                attempts.incrementAndGet();
                return List.of();
            })) {
            TestUtils.waitForCondition(() -> {
                resolver.ensureBootstrapped(time.milliseconds(), metadataUpdater);
                return attempts.get() == 1;
            }, "Initial bootstrap resolution did not complete");

            time.sleep(configuration.bootstrapResolveTimeoutMs);
            resolver.ensureBootstrapped(time.milliseconds(), metadataUpdater);
        }

        verify(metadataUpdater).bootstrapFailed(any(BootstrapResolutionException.class));
        verify(metadataUpdater, never()).bootstrap(any());
    }

    @Test
    public void testInterruptedResolutionIsCancelled() throws Exception {
        MockTime time = new MockTime();
        MetadataUpdater metadataUpdater = metadataUpdater();
        CountDownLatch resolutionStarted = new CountDownLatch(1);
        CountDownLatch resolutionMayFinish = new CountDownLatch(1);

        try (BootstrapServerResolver resolver = new BootstrapServerResolver(
            configuration(1000, 10),
            time,
            new LogContext(),
            () -> {
                resolutionStarted.countDown();
                try {
                    resolutionMayFinish.await();
                } catch (InterruptedException e) {
                    // Cancellation may interrupt the resolver while the test is releasing the blocked lookup.
                    Thread.currentThread().interrupt();
                }
                return RESOLVED_ADDRESSES;
            })) {
            TestUtils.waitForCondition(() -> resolutionStarted.getCount() == 0,
                "Bootstrap resolution did not start");

            Thread.currentThread().interrupt();
            try {
                assertThrows(InterruptException.class,
                    () -> resolver.ensureBootstrapped(time.milliseconds(), metadataUpdater));
            } finally {
                Thread.interrupted();
                resolutionMayFinish.countDown();
            }
        }

        verify(metadataUpdater, never()).bootstrap(any());
    }

    private static BootstrapServerResolver newResolver(MockTime time,
                                                       BootstrapServerResolver.AddressResolver addressResolver) {
        return new BootstrapServerResolver(configuration(1000, CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MS),
            time,
            new LogContext(),
            addressResolver);
    }

    private static BootstrapConfiguration configuration(long timeoutMs, long retryBackoffMs) {
        return BootstrapConfiguration.enabled(
            BOOTSTRAP_SERVERS,
            ClientDnsLookup.USE_ALL_DNS_IPS,
            timeoutMs,
            retryBackoffMs);
    }

    private static MetadataUpdater metadataUpdater() {
        MetadataUpdater metadataUpdater = mock(MetadataUpdater.class);
        when(metadataUpdater.fetchNodes()).thenReturn(List.of(new Node(0, "broker", 9092)));
        when(metadataUpdater.isBootstrapped()).thenReturn(false);
        return metadataUpdater;
    }
}
