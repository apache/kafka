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
package org.apache.kafka.connect.util;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.Callable;

@RunWith(PowerMockRunner.class)
public class RetryUtilTest {

    private Callable<String> mockCallable;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        mockCallable = Mockito.mock(Callable.class);
    }

    @Test
    public void testSuccess() throws Exception {
        Mockito.when(mockCallable.call()).thenReturn("success");
        assertEquals("success", RetryUtil.retry(mockCallable, 10, 100));
        Mockito.verify(mockCallable, Mockito.times(1)).call();
    }

    @Test
    public void testExhaustingRetries() throws Exception {
        Mockito.when(mockCallable.call()).thenThrow(new TimeoutException());
        ConnectException e = assertThrows(ConnectException.class,
                () -> RetryUtil.retry(mockCallable, 10, 100));
        // Expecting 11 calls: 1 call for the first execution, and 10 retry calls
        Mockito.verify(mockCallable, Mockito.times(11)).call();
    }

    @Test
    public void retriesEventuallySucceed() throws Exception {
        Mockito.when(mockCallable.call())
                .thenThrow(new TimeoutException())
                .thenThrow(new TimeoutException())
                .thenThrow(new TimeoutException())
                .thenReturn("success");

        assertEquals("success", RetryUtil.retry(mockCallable, 10, 100));
        Mockito.verify(mockCallable, Mockito.times(4)).call();
    }

    @Test
    public void failWithNonRetriableException() throws Exception {
        Mockito.when(mockCallable.call())
                .thenThrow(new TimeoutException("timeout"))
                .thenThrow(new TimeoutException("timeout"))
                .thenThrow(new TimeoutException("timeout"))
                .thenThrow(new TimeoutException("timeout"))
                .thenThrow(new TimeoutException("timeout"))
                .thenThrow(new NullPointerException("Non retriable"));
        NullPointerException e = assertThrows(NullPointerException.class,
                () -> RetryUtil.retry(mockCallable, 10, 100));
        assertEquals("Non retriable", e.getMessage());
        Mockito.verify(mockCallable, Mockito.times(6)).call();
    }

    @Test
    public void noRetryAndSucceed() throws Exception {
        Mockito.when(mockCallable.call()).thenReturn("success");

        assertEquals("success", RetryUtil.retry(mockCallable, 0, 100));
        Mockito.verify(mockCallable, Mockito.times(1)).call();
    }

    @Test
    public void noRetryAndFailed() throws Exception {
        Mockito.when(mockCallable.call()).thenThrow(new TimeoutException("timeout exception"));

        TimeoutException e = assertThrows(TimeoutException.class,
                () -> RetryUtil.retry(mockCallable, 0, 100));
        Mockito.verify(mockCallable, Mockito.times(1)).call();
        assertEquals("timeout exception", e.getMessage());
    }

    @Test
    public void testNoBackoffTimeAndSucceed() throws Exception {
        Mockito.when(mockCallable.call())
                .thenThrow(new TimeoutException())
                .thenThrow(new TimeoutException())
                .thenThrow(new TimeoutException())
                .thenReturn("success");

        assertEquals("success", RetryUtil.retry(mockCallable, 10, 0));
        Mockito.verify(mockCallable, Mockito.times(4)).call();
    }

    @Test
    public void testNoBackoffTimeAndFail() throws Exception {
        Mockito.when(mockCallable.call()).thenThrow(new TimeoutException("timeout exception"));

        ConnectException e  = assertThrows(ConnectException.class,
                () -> RetryUtil.retry(mockCallable, 10, 0));
        Mockito.verify(mockCallable, Mockito.times(11)).call();
        assertEquals("Fail to retry the task after 11 attempts.  Reason: timeout exception", e.getMessage());
    }
}
