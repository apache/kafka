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
package org.apache.kafka.coordinator.common.runtime;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MockCoordinatorExecutorTest {
    private static final String TASK_KEY = "task";

    /**
     * The arguments of a call to the operation of a task.
     */
    private record OperationCall(Object result, Throwable exception) { }

    @Test
    public void testPollWhenTaskFails() {
        MockCoordinatorExecutor<String> executor = new MockCoordinatorExecutor<>();
        Exception exception = new Exception("Oh no!");
        List<OperationCall> calls = new ArrayList<>();

        executor.schedule(
            TASK_KEY,
            () -> {
                throw exception;
            },
            (result, ex) -> {
                calls.add(new OperationCall(result, ex));
                return new CoordinatorResult<>(List.of("record"));
            }
        );

        assertEquals(
            List.of(new MockCoordinatorExecutor.ExecutorResult<>(
                TASK_KEY,
                new CoordinatorResult<>(List.of("record"))
            )),
            executor.poll()
        );
        assertEquals(List.of(new OperationCall(null, exception)), calls);
    }

    @Test
    public void testPollWhenOperationFails() {
        MockCoordinatorExecutor<String> executor = new MockCoordinatorExecutor<>();
        IllegalStateException exception = new IllegalStateException("Oh no!");
        List<OperationCall> calls = new ArrayList<>();

        executor.schedule(
            TASK_KEY,
            () -> "result",
            (result, ex) -> {
                calls.add(new OperationCall(result, ex));
                // Like the operations of the group coordinator, the operation returns a result
                // when it gets an exception.
                if (ex != null) return new CoordinatorResult<>(List.of());
                throw exception;
            }
        );

        assertSame(exception, assertThrows(IllegalStateException.class, executor::poll));
        assertEquals(List.of(new OperationCall("result", null)), calls);
    }
}
