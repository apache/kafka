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

import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.OptionalInt;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.controller.errors.ControllerExceptions.isTimeoutException;
import static org.apache.kafka.controller.errors.ControllerExceptions.newPreMigrationException;
import static org.apache.kafka.controller.errors.ControllerExceptions.newWrongControllerException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class ControllerExceptionsTest {
    @Test
    public void testTimeoutExceptionIsTimeoutException() {
        assertTrue(isTimeoutException(new TimeoutException()));
    }

    @Test
    public void testWrappedTimeoutExceptionIsTimeoutException() {
        assertTrue(isTimeoutException(
            new ExecutionException("execution exception",
                new TimeoutException())));
    }

    @Test
    public void testRuntimeExceptionIsNotTimeoutException() {
        assertFalse(isTimeoutException(new RuntimeException()));
    }

    @Test
    public void testWrappedRuntimeExceptionIsNotTimeoutException() {
        assertFalse(isTimeoutException(new ExecutionException(new RuntimeException())));
    }

    @Test
    public void testTopicExistsExceptionIsNotTimeoutException() {
        assertFalse(isTimeoutException(new TopicExistsException("Topic exists.")));
    }

    @Test
    public void testExecutionExceptionWithNullCauseIsNotTimeoutException() {
        assertFalse(isTimeoutException(new ExecutionException(null)));
    }

    @Test
    public void testNewPreMigrationExceptionWithNoController() {
        assertExceptionsMatch(new NotControllerException("No controller appears to be active."),
            newPreMigrationException(OptionalInt.empty()));
    }

    @Test
    public void testNewPreMigrationExceptionWithActiveController() {
        assertExceptionsMatch(new NotControllerException("The controller is in pre-migration mode."),
            newPreMigrationException(OptionalInt.of(1)));
    }

    @Test
    public void testNewWrongControllerExceptionWithNoController() {
        assertExceptionsMatch(new NotControllerException("No controller appears to be active."),
            newWrongControllerException(OptionalInt.empty()));
    }

    @Test
    public void testNewWrongControllerExceptionWithActiveController() {
        assertExceptionsMatch(new NotControllerException("The active controller appears to be node 1."),
            newWrongControllerException(OptionalInt.of(1)));
    }

    static void assertExceptionsMatch(Throwable a, Throwable b) {
        assertEquals(a.getClass(), b.getClass());
        assertEquals(a.getMessage(), b.getMessage());
        if (a.getCause() != null) {
            assertNotNull(b.getCause());
            assertExceptionsMatch(a.getCause(), b.getCause());
        } else {
            assertNull(b.getCause());
        }
    }
}
