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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
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
        Time time = new MockTime();
        HttpsJwks httpsJwks = spyHttpsJwks();

        try (RefreshingHttpsJwks refreshingHttpsJwks = getRefreshingHttpsJwks(time, httpsJwks)) {
            refreshingHttpsJwks.init();
            verify(httpsJwks, times(1)).refresh();
            assertTrue(refreshingHttpsJwks.maybeExpediteRefresh(keyId));
            verify(httpsJwks, times(1)).refresh();
        }
    }

    /**
     * Test that a key previously scheduled for refresh will <b>not</b> be scheduled a second time
     * if it's requested right away.
     */

    @Test
    public void testMaybeExpediteRefreshNoDelay() throws Exception {
        String keyId = "abc123";
        Time time = new MockTime();
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

        Time time = new MockTime();
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
        Time time = MockTime.SYSTEM;    // Unfortunately, we can't mock time here because the
                                        // scheduled executor doesn't respect it.
        HttpsJwks httpsJwks = spyHttpsJwks();

        try (RefreshingHttpsJwks refreshingHttpsJwks = getRefreshingHttpsJwks(time, httpsJwks)) {
            refreshingHttpsJwks.init();
            verify(httpsJwks, times(1)).refresh();
            assertTrue(refreshingHttpsJwks.maybeExpediteRefresh(keyId));
            time.sleep(REFRESH_MS + 1);
            verify(httpsJwks, times(3)).refresh();
            assertFalse(refreshingHttpsJwks.maybeExpediteRefresh(keyId));
        }
    }

    private void assertMaybeExpediteRefreshWithDelay(long sleepDelay, boolean shouldBeScheduled) throws Exception {
        String keyId = "abc123";
        Time time = new MockTime();
        HttpsJwks httpsJwks = spyHttpsJwks();

        try (RefreshingHttpsJwks refreshingHttpsJwks = getRefreshingHttpsJwks(time, httpsJwks)) {
            refreshingHttpsJwks.init();
            assertTrue(refreshingHttpsJwks.maybeExpediteRefresh(keyId));
            time.sleep(sleepDelay);
            assertEquals(shouldBeScheduled, refreshingHttpsJwks.maybeExpediteRefresh(keyId));
        }
    }

    private RefreshingHttpsJwks getRefreshingHttpsJwks(final Time time, final HttpsJwks httpsJwks) {
        return new RefreshingHttpsJwks(time, httpsJwks, REFRESH_MS, RETRY_BACKOFF_MS, RETRY_BACKOFF_MAX_MS);
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

}
