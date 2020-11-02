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
package org.apache.kafka.raft;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertFalse;

class MockExpirationServiceTest {

    private final MockTime time = new MockTime();
    private final MockExpirationService expirationService = new MockExpirationService(time);

    @Test
    public void testFailAfter() {
        CompletableFuture<Object> future1 = expirationService.failAfter(50);
        CompletableFuture<Object> future2 = expirationService.failAfter(25);
        CompletableFuture<Object> future3 = expirationService.failAfter(75);
        CompletableFuture<Object> future4 = expirationService.failAfter(50);

        time.sleep(25);
        TestUtils.assertFutureThrows(future2, TimeoutException.class);
        assertFalse(future1.isDone());
        assertFalse(future3.isDone());
        assertFalse(future4.isDone());

        time.sleep(25);
        TestUtils.assertFutureThrows(future1, TimeoutException.class);
        TestUtils.assertFutureThrows(future4, TimeoutException.class);
        assertFalse(future3.isDone());

        time.sleep(25);
        TestUtils.assertFutureThrows(future3, TimeoutException.class);
    }

}