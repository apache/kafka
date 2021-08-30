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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;

public class RetryTest extends OAuthBearerTest {

    @Test
    public void test() throws IOException {
        Exception[] attempts = new Exception[] {
            new IOException("pretend connect error"),
            new IOException("pretend timeout error"),
            new IOException("pretend read error"),
            null    // success!
        };
        long retryWaitMs = 1000;
        long maxWaitMs = 10000;
        Retryable<String> call = createRetryable(attempts);

        Time time = new MockTime(0, 0, 0);
        assertEquals(0L, time.milliseconds());
        Retry<String> r = new Retry<>(time, attempts.length, retryWaitMs, maxWaitMs);
        r.execute(call);

        long secondWait = retryWaitMs * 2;
        long thirdWait = retryWaitMs * 4;
        long totalWait = retryWaitMs + secondWait + thirdWait;
        assertEquals(totalWait, time.milliseconds());
        assertEquals(attempts.length, r.getAttemptsMade());
    }

    @Test
    public void testIOExceptionFailure() {
        Exception[] attempts = new Exception[] {
            new IOException("pretend connect error"),
            new IOException("pretend timeout error"),
            new IOException("pretend read error")
        };
        long retryWaitMs = 1000;
        long maxWaitMs = 10000;
        Retryable<String> call = createRetryable(attempts);

        Time time = new MockTime(0, 0, 0);
        assertEquals(0L, time.milliseconds());
        Retry<String> r = new Retry<>(time, attempts.length, retryWaitMs, maxWaitMs);

        assertThrows(IOException.class, () -> r.execute(call));

        long secondWait = retryWaitMs * 2;
        long totalWait = retryWaitMs + secondWait;
        assertEquals(totalWait, time.milliseconds());
        assertEquals(attempts.length, r.getAttemptsMade());
    }

    @Test
    public void testRuntimeExceptionFailureOnLastAttempt() {
        Exception[] attempts = new Exception[] {
            new IOException("pretend connect error"),
            new IOException("pretend timeout error"),
            new NullPointerException("pretend JSON node /userId in response is null")
        };
        long retryWaitMs = 1000;
        long maxWaitMs = 10000;
        Retryable<String> call = createRetryable(attempts);

        Time time = new MockTime(0, 0, 0);
        assertEquals(0L, time.milliseconds());
        Retry<String> r = new Retry<>(time, attempts.length, retryWaitMs, maxWaitMs);

        assertThrows(RuntimeException.class, () -> r.execute(call));

        long secondWait = retryWaitMs * 2;
        long totalWait = retryWaitMs + secondWait;
        assertEquals(totalWait, time.milliseconds());
        assertEquals(attempts.length, r.getAttemptsMade());
    }

    @Test
    public void testRuntimeExceptionFailureOnFirstAttempt() {
        Exception[] attempts = new Exception[] {
            new NullPointerException("pretend JSON node /userId in response is null"),
            null
        };
        long retryWaitMs = 1000;
        long maxWaitMs = 10000;
        Retryable<String> call = createRetryable(attempts);

        Time time = new MockTime(0, 0, 0);
        assertEquals(0L, time.milliseconds());
        Retry<String> r = new Retry<>(time, attempts.length, retryWaitMs, maxWaitMs);

        assertThrows(RuntimeException.class, () -> r.execute(call));

        assertEquals(0, time.milliseconds());   // No wait time because we...
        assertEquals(1, r.getAttemptsMade());   // ...failed on the first attempt
    }

    @Test
    public void testReuseRetry() throws IOException {
        Exception[] attempts = new Exception[] {
            null
        };
        long retryWaitMs = 1000;
        long maxWaitMs = 10000;

        Time time = new MockTime(0, 0, 0);
        assertEquals(0L, time.milliseconds());
        Retry<String> r = new Retry<>(time, attempts.length, retryWaitMs, maxWaitMs);

        r.execute(createRetryable(attempts));

        assertEquals(0, time.milliseconds());   // No wait time because we...
        assertEquals(1, r.getAttemptsMade());   // ...succeeded on the first attempt

        // Don't try to use again even if we succeeded!
        assertThrows(IllegalStateException.class, () -> r.execute(createRetryable(attempts)));
    }

    @Test
    public void testUseMaxTimeout() throws IOException {
        Exception[] attempts = new Exception[] {
            new IOException("pretend connect error"),
            new IOException("pretend timeout error"),
            new IOException("pretend read error")
        };
        long retryWaitMs = 5000;
        long maxWaitMs = 5000;
        Retryable<String> call = createRetryable(attempts);

        Time time = new MockTime(0, 0, 0);
        assertEquals(0L, time.milliseconds());
        Retry<String> r = new Retry<>(time, attempts.length, retryWaitMs, maxWaitMs);

        assertThrows(IOException.class, () -> r.execute(call));

        // Here the total wait is the same value of maxWaitMs between both of our
        // failures.
        long totalWait = retryWaitMs + retryWaitMs;
        assertEquals(totalWait, time.milliseconds());
        assertEquals(attempts.length, r.getAttemptsMade());
    }

}
