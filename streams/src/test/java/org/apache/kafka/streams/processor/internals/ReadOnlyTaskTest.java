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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.TaskId;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.standbyTask;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.statefulTask;
import static org.apache.kafka.test.StreamsTestUtils.TaskBuilder.statelessTask;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

class ReadOnlyTaskTest {

    private final List<String> readOnlyMethods = new LinkedList<String>() {
        {
            add("needsInitializationOrRestoration");
            add("inputPartitions");
            add("changelogPartitions");
            add("commitRequested");
            add("commitNeeded");
            add("isActive");
            add("changelogOffsets");
            add("state");
            add("id");
            add("store");
        }
    };

    private final List<String> objectMethods = new LinkedList<String>() {
        {
            add("wait");
            add("equals");
            add("getClass");
            add("hashCode");
            add("notify");
            add("notifyAll");
            add("toString");
        }
    };

    final Task task = statelessTask(new TaskId(1, 0)).build();

    @Test
    public void shouldDelegateNeedsInitializationOrRestoration() {
        final ReadOnlyTask readOnlyTask = new ReadOnlyTask(task);

        readOnlyTask.needsInitializationOrRestoration();

        verify(task).needsInitializationOrRestoration();
    }

    @Test
    public void shouldDelegateId() {
        final ReadOnlyTask readOnlyTask = new ReadOnlyTask(task);

        readOnlyTask.id();

        verify(task).id();
    }

    @Test
    public void shouldDelegateIsActive() {
        final ReadOnlyTask readOnlyTask = new ReadOnlyTask(task);

        readOnlyTask.isActive();

        verify(task).isActive();
    }

    @Test
    public void shouldDelegateInputPartitions() {
        final ReadOnlyTask readOnlyTask = new ReadOnlyTask(task);

        readOnlyTask.inputPartitions();

        verify(task).inputPartitions();
    }

    @Test
    public void shouldDelegateChangelogPartitions() {
        final ReadOnlyTask readOnlyTask = new ReadOnlyTask(task);

        readOnlyTask.changelogPartitions();

        verify(task).changelogPartitions();
    }

    @Test
    public void shouldDelegateCommitRequested() {
        final ReadOnlyTask readOnlyTask = new ReadOnlyTask(task);

        readOnlyTask.commitRequested();

        verify(task).commitRequested();
    }

    @Test
    public void shouldDelegateState() {
        final ReadOnlyTask readOnlyTask = new ReadOnlyTask(task);

        readOnlyTask.state();

        verify(task).state();
    }

    @Test
    public void shouldDelegateCommitNeededIfStandby() {
        final StandbyTask standbyTask =
            standbyTask(new TaskId(1, 0), Set.of(new TopicPartition("topic", 0))).build();
        final ReadOnlyTask readOnlyTask = new ReadOnlyTask(standbyTask);

        readOnlyTask.commitNeeded();

        verify(standbyTask).commitNeeded();
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionForCommitNeededIfActive() {
        final StreamTask statefulTask =
            statefulTask(new TaskId(1, 0), Set.of(new TopicPartition("topic", 0))).build();
        final ReadOnlyTask readOnlyTask = new ReadOnlyTask(statefulTask);

        final Exception exception = assertThrows(UnsupportedOperationException.class, readOnlyTask::commitNeeded);

        assertEquals("This task is read-only", exception.getMessage());
    }

    @Test
    public void shouldThrowUnsupportedOperationExceptionForForbiddenMethods() {
        final ReadOnlyTask readOnlyTask = new ReadOnlyTask(task);
        for (final Method method : ReadOnlyTask.class.getMethods()) {
            final String methodName = method.getName();
            if (!readOnlyMethods.contains(methodName) && !objectMethods.contains(methodName)) {
                shouldThrowUnsupportedOperationException(readOnlyTask, method);
            }
        }

    }

    private void shouldThrowUnsupportedOperationException(final ReadOnlyTask readOnlyTask,
                                                          final Method method) {
        final Exception exception = assertThrows(
            UnsupportedOperationException.class,
            () -> {
                try {
                    method.invoke(readOnlyTask, getParameters(method.getParameterTypes()));
                } catch (final InvocationTargetException invocationTargetException) {
                    throw invocationTargetException.getCause();
                }
            },
            "Something unexpected happened during invocation of method '" + method.getName() + "'!"
        );
        assertEquals("This task is read-only", exception.getMessage());
    }

    private Object[] getParameters(final Class<?>[] parameterTypes) throws Exception {
        final Object[] parameters = new Object[parameterTypes.length];

        for (int i = 0; i < parameterTypes.length; ++i) {
            switch (parameterTypes[i].getName()) {
                case "boolean":
                    parameters[i] = true;
                    break;
                case "long":
                    parameters[i] = 0;
                    break;
                case "java.util.Set":
                    parameters[i] = Collections.emptySet();
                    break;
                case "java.util.Collection":
                    parameters[i] = Collections.emptySet();
                    break;
                case "java.util.Map":
                    parameters[i] = Collections.emptyMap();
                    break;
                case "org.apache.kafka.common.TopicPartition":
                    parameters[i] = new TopicPartition("topic", 0);
                    break;
                case "org.apache.kafka.clients.consumer.OffsetAndMetadata":
                    parameters[i] = new OffsetAndMetadata(0, Optional.empty(), "");
                    break;
                case "java.lang.Exception":
                    parameters[i] = new IllegalStateException();
                    break;
                case "java.util.function.Consumer":
                    parameters[i] = (Consumer) ignored -> { };
                    break;
                case "java.lang.Iterable":
                    parameters[i] = Collections.emptySet();
                    break;
                case "org.apache.kafka.common.utils.Time":
                    parameters[i] = Time.SYSTEM;
                    break;
                default:
                    parameters[i] = parameterTypes[i].getConstructor().newInstance();
            }
        }

        return parameters;
    }
}