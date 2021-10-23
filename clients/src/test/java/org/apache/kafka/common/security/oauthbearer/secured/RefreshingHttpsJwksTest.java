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

package org.apache.kafka.common.security.oauthbearer.secured;

import static org.apache.kafka.common.security.oauthbearer.secured.RefreshingHttpsJwks.MISSING_KEY_ID_CACHE_IN_FLIGHT_MS;
import static org.apache.kafka.common.security.oauthbearer.secured.RefreshingHttpsJwks.MISSING_KEY_ID_MAX_KEY_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.utils.MockTime;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwk.JsonWebKey;
import org.junit.jupiter.api.Test;

public class RefreshingHttpsJwksTest extends OAuthBearerTest {

    public static final int REFRESH_MS = 10000;

    /**
     * Test that a key not previously scheduled for refresh will be scheduled without a refresh.
     */

    @Test
    public void test() throws Exception {
        String keyId = "abc123";
        MockTime time = new MockTime();
        HttpsJwks httpsJwks = mockHttpsJwks();

        try (RefreshingHttpsJwks refreshingHttpsJwks = new RefreshingHttpsJwks(time, httpsJwks, REFRESH_MS)) {
            refreshingHttpsJwks.init();
            assertTrue(refreshingHttpsJwks.maybeScheduleRefreshForMissingKeyId(keyId));
            verify(httpsJwks, times(0)).refresh();
        }
    }

    /**
     * Test that a key previously scheduled for refresh will <b>not</b> be scheduled a second time
     * if it's requested right away.
     */

    @Test
    public void testScheduleRefreshForMissingKeyNoDelay() throws Exception {
        String keyId = "abc123";
        MockTime time = new MockTime();
        HttpsJwks httpsJwks = mockHttpsJwks();

        try (RefreshingHttpsJwks refreshingHttpsJwks = new RefreshingHttpsJwks(time, httpsJwks, REFRESH_MS)) {
            refreshingHttpsJwks.init();
            assertTrue(refreshingHttpsJwks.maybeScheduleRefreshForMissingKeyId(keyId));
            assertFalse(refreshingHttpsJwks.maybeScheduleRefreshForMissingKeyId(keyId));
        }
    }

    /**
     * Test that a key previously scheduled for refresh <b>will</b> be scheduled a second time
     * if it's requested after the delay.
     */

    @Test
    public void testScheduleRefreshForMissingKeyDelays() throws Exception {
        assertScheduleRefreshForMissingKeyWithDelay(MISSING_KEY_ID_CACHE_IN_FLIGHT_MS - 1, false);
        assertScheduleRefreshForMissingKeyWithDelay(MISSING_KEY_ID_CACHE_IN_FLIGHT_MS, true);
        assertScheduleRefreshForMissingKeyWithDelay(MISSING_KEY_ID_CACHE_IN_FLIGHT_MS + 1, true);
    }

    @Test
    public void testLongKey() throws Exception {
        char[] keyIdChars = new char[MISSING_KEY_ID_MAX_KEY_LENGTH + 1];
        Arrays.fill(keyIdChars, '0');
        String keyId = new String(keyIdChars);

        MockTime time = new MockTime();
        HttpsJwks httpsJwks = mockHttpsJwks();

        try (RefreshingHttpsJwks refreshingHttpsJwks = new RefreshingHttpsJwks(time, httpsJwks, REFRESH_MS)) {
            refreshingHttpsJwks.init();
            assertFalse(refreshingHttpsJwks.maybeScheduleRefreshForMissingKeyId(keyId));
            verify(httpsJwks, times(0)).refresh();
        }
    }

    private void assertScheduleRefreshForMissingKeyWithDelay(long sleepDelay, boolean shouldBeScheduled) throws Exception {
        String keyId = "abc123";
        MockTime time = new MockTime();
        HttpsJwks httpsJwks = mockHttpsJwks();

        try (RefreshingHttpsJwks refreshingHttpsJwks = new RefreshingHttpsJwks(time, httpsJwks, REFRESH_MS)) {
            refreshingHttpsJwks.init();
            assertTrue(refreshingHttpsJwks.maybeScheduleRefreshForMissingKeyId(keyId));
            time.sleep(sleepDelay);
            assertEquals(shouldBeScheduled, refreshingHttpsJwks.maybeScheduleRefreshForMissingKeyId(keyId));
        }
    }

    private HttpsJwks mockHttpsJwks() throws Exception {
        return mockHttpsJwks(Collections.emptyList());
    }

    private HttpsJwks mockHttpsJwks(List<JsonWebKey> objects) throws Exception {
        HttpsJwks httpsJwks = mock(HttpsJwks.class);
        when(httpsJwks.getJsonWebKeys()).thenReturn(objects);
        when(httpsJwks.getLocation()).thenReturn("https://www.example.com");
        return httpsJwks;
    }

    private JsonWebKey mockJsonWebKey(String keyId) {
        JsonWebKey jwk = mock(JsonWebKey.class);
        when(jwk.getKeyId()).thenReturn(keyId);
        return jwk;
    }

}
