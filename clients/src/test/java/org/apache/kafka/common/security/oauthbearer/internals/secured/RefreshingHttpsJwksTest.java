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

package org.apache.kafka.common.security.oauthbearer.internals.secured;

import static org.apache.kafka.common.security.oauthbearer.internals.secured.RefreshingHttpsJwks.MISSING_KEY_ID_CACHE_IN_FLIGHT_MS;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.RefreshingHttpsJwks.MISSING_KEY_ID_MAX_KEY_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.AbstractMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.MockTime;
import org.jose4j.http.SimpleResponse;
import org.jose4j.jwk.HttpsJwks;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class RefreshingHttpsJwksTest extends OAuthBearerTest {

    private static final int REFRESH_MS = 5000;

    private static final int RETRY_BACKOFF_MS = 50;

    private static final int RETRY_BACKOFF_MAX_MS = 2000;

    /**
     * Test that a key not previously scheduled for refresh will be scheduled without a refresh.
     */

    @Test
    public void testBasicScheduleRefresh() throws Exception {
        String keyId = "abc123";
        MockTime time = new MockTime();
        HttpsJwks httpsJwks = spyHttpsJwks();

       // we use mocktime here to ensure that scheduled refresh _doesn't_ run and update the invocation count
       // we expect httpsJwks.refresh() to be invoked twice, once from init() and maybeExpediteRefresh() each
        try (RefreshingHttpsJwks refreshingHttpsJwks = getRefreshingHttpsJwks(time, httpsJwks)) {
            refreshingHttpsJwks.init();
            verify(httpsJwks, times(1)).refresh();
            assertTrue(refreshingHttpsJwks.maybeExpediteRefresh(keyId));
            verify(httpsJwks, times(2)).refresh();
        }
    }

    /**
     * Test that a key previously scheduled for refresh will <b>not</b> be scheduled a second time
     * if it's requested right away.
     */

    @Test
    public void testMaybeExpediteRefreshNoDelay() throws Exception {
        String keyId = "abc123";
        MockTime time = new MockTime();
        HttpsJwks httpsJwks = spyHttpsJwks();

        try (RefreshingHttpsJwks refreshingHttpsJwks = getRefreshingHttpsJwks(time, httpsJwks)) {
            refreshingHttpsJwks.init();
            assertTrue(refreshingHttpsJwks.maybeExpediteRefresh(keyId));
            assertFalse(refreshingHttpsJwks.maybeExpediteRefresh(keyId));
        }
    }

    /**
     * Test that a key previously scheduled for refresh <b>will</b> be scheduled a second time
     * if it's requested after the delay.
     */

    @Test
    public void testMaybeExpediteRefreshDelays() throws Exception {
        assertMaybeExpediteRefreshWithDelay(MISSING_KEY_ID_CACHE_IN_FLIGHT_MS - 1, false);
        assertMaybeExpediteRefreshWithDelay(MISSING_KEY_ID_CACHE_IN_FLIGHT_MS, true);
        assertMaybeExpediteRefreshWithDelay(MISSING_KEY_ID_CACHE_IN_FLIGHT_MS + 1, true);
    }

    /**
     * Test that a "long key" will not be looked up because the key ID is too long.
     */

    @Test
    public void testLongKey() throws Exception {
        char[] keyIdChars = new char[MISSING_KEY_ID_MAX_KEY_LENGTH + 1];
        Arrays.fill(keyIdChars, '0');
        String keyId = new String(keyIdChars);

        MockTime time = new MockTime();
        HttpsJwks httpsJwks = spyHttpsJwks();

        try (RefreshingHttpsJwks refreshingHttpsJwks = getRefreshingHttpsJwks(time, httpsJwks)) {
            refreshingHttpsJwks.init();
            verify(httpsJwks, times(1)).refresh();
            assertFalse(refreshingHttpsJwks.maybeExpediteRefresh(keyId));
            verify(httpsJwks, times(1)).refresh();
        }
    }

    /**
     * Test that if we ask to load a missing key, and then we wait past the sleep time that it will
     * call refresh to load the key.
     */

    @Test
    public void testSecondaryRefreshAfterElapsedDelay() throws Exception {
        String keyId = "abc123";
        MockTime time = new MockTime();
        HttpsJwks httpsJwks = spyHttpsJwks();

        try (RefreshingHttpsJwks refreshingHttpsJwks = getRefreshingHttpsJwks(time, httpsJwks)) {
            refreshingHttpsJwks.init();
            // We refresh once at the initialization time from getJsonWebKeys.
            verify(httpsJwks, times(1)).refresh();
            assertTrue(refreshingHttpsJwks.maybeExpediteRefresh(keyId));
            verify(httpsJwks, times(2)).refresh();
            time.sleep(REFRESH_MS + 1);
            verify(httpsJwks, times(3)).refresh();
            assertFalse(refreshingHttpsJwks.maybeExpediteRefresh(keyId));
        }
    }

    private ScheduledExecutorService mockExecutorService(MockTime time) {
        MockExecutorService mockExecutorService = new MockExecutorService(time);
        ScheduledExecutorService executorService = Mockito.mock(ScheduledExecutorService.class);
        Mockito.doAnswer(invocation -> {
            Runnable command = invocation.getArgument(0, Runnable.class);
            long delay = invocation.getArgument(1, Long.class);
            TimeUnit unit = invocation.getArgument(2, TimeUnit.class);
            return mockExecutorService.schedule(() -> {
                command.run();
                return null;
            }, unit.toMillis(delay), null);
        }).when(executorService).schedule(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.any(TimeUnit.class));
        Mockito.doAnswer(invocation -> {
            Runnable command = invocation.getArgument(0, Runnable.class);
            long initialDelay = invocation.getArgument(1, Long.class);
            long period = invocation.getArgument(2, Long.class);
            TimeUnit unit = invocation.getArgument(3, TimeUnit.class);
            return mockExecutorService.schedule(() -> {
                command.run();
                return null;
            }, unit.toMillis(initialDelay), period);
        }).when(executorService).scheduleAtFixedRate(Mockito.any(Runnable.class), Mockito.anyLong(), Mockito.anyLong(), Mockito.any(TimeUnit.class));
        return executorService;
    }

    private void assertMaybeExpediteRefreshWithDelay(long sleepDelay, boolean shouldBeScheduled) throws Exception {
        String keyId = "abc123";
        MockTime time = new MockTime();
        HttpsJwks httpsJwks = spyHttpsJwks();

        try (RefreshingHttpsJwks refreshingHttpsJwks = getRefreshingHttpsJwks(time, httpsJwks)) {
            refreshingHttpsJwks.init();
            assertTrue(refreshingHttpsJwks.maybeExpediteRefresh(keyId));
            time.sleep(sleepDelay);
            assertEquals(shouldBeScheduled, refreshingHttpsJwks.maybeExpediteRefresh(keyId));
        }
    }

    private RefreshingHttpsJwks getRefreshingHttpsJwks(final MockTime time, final HttpsJwks httpsJwks) {
        return new RefreshingHttpsJwks(time, httpsJwks, REFRESH_MS, RETRY_BACKOFF_MS, RETRY_BACKOFF_MAX_MS, mockExecutorService(time));
    }

    /**
     * We *spy* (not *mock*) the {@link HttpsJwks} instance because we want to have it
     * _partially mocked_ to determine if it's calling its internal refresh method. We want to
     * make sure it *doesn't* do that when we call our getJsonWebKeys() method on
     * {@link RefreshingHttpsJwks}.
     */

    private HttpsJwks spyHttpsJwks() {
        HttpsJwks httpsJwks = new HttpsJwks("https://www.example.com");

        SimpleResponse simpleResponse = new SimpleResponse() {
            @Override
            public int getStatusCode() {
                return 200;
            }

            @Override
            public String getStatusMessage() {
                return "OK";
            }

            @Override
            public Collection<String> getHeaderNames() {
                return Collections.emptyList();
            }

            @Override
            public List<String> getHeaderValues(String name) {
                return Collections.emptyList();
            }

            @Override
            public String getBody() {
                return "{\"keys\": []}";
            }
        };

        httpsJwks.setSimpleHttpGet(l -> simpleResponse);

        return Mockito.spy(httpsJwks);
    }

    /**
     * A mock ScheduledExecutorService just for the test. Note that this is not a generally reusable mock as it does not
     * implement some interfaces like scheduleWithFixedDelay, etc. And it does not return ScheduledFuture correctly.
     */
    private static class MockExecutorService implements MockTime.Listener {
        private final MockTime time;

        private final TreeMap<Long, List<AbstractMap.SimpleEntry<Long, KafkaFutureImpl<Long>>>> waiters = new TreeMap<>();

        public MockExecutorService(MockTime time) {
            this.time = time;
            time.addListener(this);
        }

        /**
         * The actual execution and rescheduling logic. Check all internal tasks to see if any one reaches its next
         * execution point, call it and optionally reschedule it if it has a specified period.
         */
        @Override
        public synchronized void onTimeUpdated() {
            long timeMs = time.milliseconds();
            while (true) {
                Map.Entry<Long, List<AbstractMap.SimpleEntry<Long, KafkaFutureImpl<Long>>>> entry = waiters.firstEntry();
                if ((entry == null) || (entry.getKey() > timeMs)) {
                    break;
                }
                for (AbstractMap.SimpleEntry<Long, KafkaFutureImpl<Long>> pair : entry.getValue()) {
                    pair.getValue().complete(timeMs);
                    if (pair.getKey() != null) {
                        addWaiter(entry.getKey() + pair.getKey(), pair.getKey(), pair.getValue());
                    }
                }
                waiters.remove(entry.getKey());
            }
        }

        /**
         * Add a task with `delayMs` and optional period to the internal waiter.
         * When `delayMs` < 0, we immediately complete the waiter. Otherwise, we add the task metadata to the waiter and
         * onTimeUpdated will take care of execute and reschedule it when it reaches its scheduled timestamp.
         *
         * @param delayMs Delay time in ms.
         * @param period  Scheduling period, null means no periodic.
         * @param waiter  A wrapper over a callable function.
         */
        private synchronized void addWaiter(long delayMs, Long period, KafkaFutureImpl<Long> waiter) {
            long timeMs = time.milliseconds();
            if (delayMs <= 0) {
                waiter.complete(timeMs);
            } else {
                long triggerTimeMs = timeMs + delayMs;
                List<AbstractMap.SimpleEntry<Long, KafkaFutureImpl<Long>>> futures =
                        waiters.computeIfAbsent(triggerTimeMs, k -> new ArrayList<>());
                futures.add(new AbstractMap.SimpleEntry<>(period, waiter));
            }
        }

        /**
         * Internal utility function for periodic or one time refreshes.
         *
         * @param period null indicates one time refresh, otherwise it is periodic.
         */
        public <T> ScheduledFuture<T> schedule(final Callable<T> callable, long delayMs, Long period) {

            KafkaFutureImpl<Long> waiter = new KafkaFutureImpl<>();
            waiter.thenApply((KafkaFuture.BaseFunction<Long, Void>) now -> {
                try {
                    callable.call();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
                return null;
            });
            addWaiter(delayMs, period, waiter);
            return null;
        }
    }

}