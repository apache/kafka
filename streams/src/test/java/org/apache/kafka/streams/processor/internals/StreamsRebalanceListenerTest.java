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

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.errors.MissingSourceTopicException;
import org.apache.kafka.streams.processor.internals.StreamThread.State;
import org.apache.kafka.streams.processor.internals.assignment.AssignorError;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

public class StreamsRebalanceListenerTest {

    private final TaskManager taskManager = mock(TaskManager.class);
    private final StreamThread streamThread = mock(StreamThread.class);
    private final AtomicInteger assignmentErrorCode = new AtomicInteger();
    private final StreamsRebalanceListener streamsRebalanceListener = new StreamsRebalanceListener(
        new MockTime(),
        taskManager,
        streamThread,
        LoggerFactory.getLogger(StreamsRebalanceListenerTest.class),
        assignmentErrorCode
    );

    @Test
    public void shouldThrowMissingSourceTopicException() {
        replay(taskManager, streamThread);
        assignmentErrorCode.set(AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.code());

        final MissingSourceTopicException exception = assertThrows(
            MissingSourceTopicException.class,
            () -> streamsRebalanceListener.onPartitionsAssigned(Collections.emptyList())
        );
        assertThat(exception.getMessage(), is("One or more source topics were missing during rebalance"));
        verify(taskManager, streamThread);
    }

    @Test
    public void shouldHandleOnPartitionAssigned() {
        taskManager.handleRebalanceComplete();
        expect(streamThread.setState(State.PARTITIONS_ASSIGNED)).andStubReturn(null);
        replay(taskManager, streamThread);
        assignmentErrorCode.set(AssignorError.NONE.code());

        streamsRebalanceListener.onPartitionsAssigned(Collections.emptyList());

        verify(taskManager, streamThread);
    }
}