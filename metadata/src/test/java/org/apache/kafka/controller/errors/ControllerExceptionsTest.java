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

package org.apache.kafka.controller.errors;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.ExecutionException;

import static org.apache.kafka.controller.errors.ControllerExceptions.isNormalTimeoutException;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class ControllerExceptionsTest {
    @Test
    public void testTimeoutExceptionIsNormalTimeoutException() {
        assertTrue(isNormalTimeoutException(
                new TimeoutException()));
    }

    @Test
    public void testWrappedTimeoutExceptionIsNormalTimeoutException() {
        assertTrue(isNormalTimeoutException(
                new ExecutionException("execution exception",
                        new TimeoutException())));
    }

    @Test
    public void testShutdownTimeoutExceptionIsNotNormalTimeoutException() {
        assertFalse(isNormalTimeoutException(
                new TimeoutException("The controller is shutting down.")));
    }

    @Test
    public void testWrappedShutdownTimeoutExceptionIsNotNormalTimeoutException() {
        assertFalse(isNormalTimeoutException(
                new ExecutionException(
                        new TimeoutException("The controller is shutting down."))));
    }

    @Test
    public void testRuntimeExceptionIsNotNormalTimeoutException() {
        assertFalse(isNormalTimeoutException(
                new RuntimeException()));
    }

    @Test
    public void testWrappedRuntimeExceptionIsNotNormalTimeoutException() {
        assertFalse(isNormalTimeoutException(
                new ExecutionException(
                        new RuntimeException())));
    }

    @Test
    public void testTopicExistsExceptionIsNotNormalTimeoutException() {
        assertFalse(isNormalTimeoutException(
                new TopicExistsException("Topic exists.")));
    }

    @Test
    public void testExecutionExceptionIsNotNormalTimeoutException() {
        assertFalse(isNormalTimeoutException(
                new ExecutionException(null)));
    }
}
