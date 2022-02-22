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
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.Callable;

@RunWith(PowerMockRunner.class)
public class RetryUtilTest {

    @Mock
    private Callable<String> mockCallable;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        mockCallable = Mockito.mock(Callable.class);
    }

    @Test
    public void TestSuccess() throws Exception {
        Mockito.when(mockCallable.call()).thenReturn("success");
        //EasyMock.expect(mockCallable.call()).andReturn("success").times(1);
        try {
            assertEquals("success", RetryUtil.retry(mockCallable, 10, 100));
        } catch (Exception e) {
            fail("Not expecting an exception: ", e.getCause());
        }
        Mockito.verify(mockCallable, Mockito.times(1)).call();
    }

    @Test
    public void TestFailingRetries() throws Exception {
        Mockito.when(mockCallable.call()).thenThrow(new TimeoutException());
        try {
            RetryUtil.retry(mockCallable, 10, 100);
            fail("Expect exception being thrown here");
        } catch (ConnectException e) {
            // expecting a connect exception
        } catch (Exception e) {
            fail("Only expecting ConnectException");
        }
        Mockito.verify(mockCallable, Mockito.times(1)).call();
    }

    @Test
    public void RetriesEventuallySucceed() throws Exception {
        Mockito.when(mockCallable.call())
                .thenThrow(new TimeoutException())
                .thenThrow(new TimeoutException())
                .thenThrow(new TimeoutException())
                .thenReturn("success");
        try {
            assertEquals("success", RetryUtil.retry(mockCallable, 10, 100));
        } catch (Exception e) {
            fail("Not expecting an exception: ", e.getCause());
        }
        Mockito.verify(mockCallable, Mockito.times(4)).call();
    }

    @Test
    public void RetriesEventuallyFail() throws Exception {
        Mockito.when(mockCallable.call())
                .thenThrow(new TimeoutException("timeout"));
        try {
            for (int i = 0; i < 10; i++) {
                RetryUtil.retry(mockCallable, 10, 100);
            }
            fail("Not expecting an exception: ");
        } catch (ConnectException e) {
           // good
        } catch (Exception e) {
            fail("Expecting ConnectException");
        }
        Mockito.verify(mockCallable, Mockito.times(10)).call();
    }

    @Test
    public void FailWithNonRetriableException() throws Exception {
        Mockito.when(mockCallable.call())
                .thenThrow(new TimeoutException("timeout"))
                .thenThrow(new TimeoutException("timeout"))
                .thenThrow(new TimeoutException("timeout"))
                .thenThrow(new TimeoutException("timeout"))
                .thenThrow(new TimeoutException("timeout"))
                .thenThrow(new NullPointerException("Non retriable"));
        try {
            for (int i = 0; i < 10; i++) {
                RetryUtil.retry(mockCallable, 10, 100);
            }
            fail("Not expecting an exception: ");
        } catch (ConnectException e) {
            fail("Should fail with NPE");
        } catch (NullPointerException e) {
            // good
        }
        Mockito.verify(mockCallable, Mockito.times(6)).call();
    }
}
