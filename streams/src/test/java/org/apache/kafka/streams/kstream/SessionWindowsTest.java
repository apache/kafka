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
package org.apache.kafka.streams.kstream;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SessionWindowsTest {

    @Test
    public void shouldSetWindowGap() {
        final long anyGap = 42L;
        assertEquals(anyGap, SessionWindows.with(anyGap).inactivityGap());
    }

    @Test
    public void shouldSetWindowRetentionTime() {
        final long anyRetentionTime = 42L;
        assertEquals(anyRetentionTime, SessionWindows.with(1).until(anyRetentionTime).maintainMs());
    }

    @Test(expected = IllegalArgumentException.class)
    public void windowSizeMustNotBeNegative() {
        SessionWindows.with(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void windowSizeMustNotBeZero() {
        SessionWindows.with(0);
    }

    @Test
    public void retentionTimeShouldBeGapIfGapIsLargerThanDefaultRetentionTime() {
        final long windowGap = 2 * Windows.DEFAULT_MAINTAIN_DURATION_MS;
        assertEquals(windowGap, SessionWindows.with(windowGap).maintainMs());
    }

    @Test
    public void retentionTimeMustNotBeNegative() {
        final SessionWindows windowSpec = SessionWindows.with(42);
        try {
            windowSpec.until(41);
            fail("should not accept retention time smaller than gap");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

}