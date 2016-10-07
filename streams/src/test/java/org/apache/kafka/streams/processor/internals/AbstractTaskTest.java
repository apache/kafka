/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Collections;

public class AbstractTaskTest {

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateExceptionOnInitializeOffsetsWhenAuthorizationException() throws Exception {
        final Consumer consumer = mockConsumer(new AuthorizationException("blah"));
        final AbstractTask task = createTask(consumer);
        task.initializeOffsetLimits();
    }

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateExceptionOnInitializeOffsetsWhenKafkaException() throws Exception {
        final Consumer consumer = mockConsumer(new KafkaException("blah"));
        final AbstractTask task = createTask(consumer);
        task.initializeOffsetLimits();
    }

    @Test(expected = WakeupException.class)
    public void shouldThrowWakeupExceptionOnInitializeOffsetsWhenWakeupException() throws Exception {
        final Consumer consumer = mockConsumer(new WakeupException());
        final AbstractTask task = createTask(consumer);
        task.initializeOffsetLimits();
    }

    private AbstractTask createTask(final Consumer consumer) {
        return new AbstractTask(new TaskId(0, 0),
                                "app",
                                Collections.singletonList(new TopicPartition("t", 0)),
                                new ProcessorTopology(Collections.<ProcessorNode>emptyList(),
                                                      Collections.<String, SourceNode>emptyMap(),
                                                      Collections.<String, SinkNode>emptyMap(),
                                                      Collections.<StateStore>emptyList(),
                                                      Collections.<String, String>emptyMap(),
                                                      Collections.<StateStore, ProcessorNode>emptyMap()
                                               ),
                                consumer,
                                consumer,
                                false,
                                new StateDirectory("app", TestUtils.tempDirectory().getPath()),
                                new ThreadCache(0)) {
            @Override
            public void commit() {
                // do nothing
            }

            @Override
            public void close() {

            }

            @Override
            public void commitOffsets() {
                // do nothing
            }
        };
    }

    private Consumer mockConsumer(final RuntimeException toThrow) {
        return new MockConsumer(OffsetResetStrategy.EARLIEST) {
            @Override
            public OffsetAndMetadata committed(final TopicPartition partition) {
                throw toThrow;
            }
        };
    }

}