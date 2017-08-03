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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

public class AbstractTaskTest {

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateExceptionOnInitializeOffsetsWhenAuthorizationException() throws Exception {
        final Consumer consumer = mockConsumer(new AuthorizationException("blah"));
        final AbstractTask task = createTask(consumer);
        task.updateOffsetLimits();
    }

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateExceptionOnInitializeOffsetsWhenKafkaException() throws Exception {
        final Consumer consumer = mockConsumer(new KafkaException("blah"));
        final AbstractTask task = createTask(consumer);
        task.updateOffsetLimits();
    }

    @Test(expected = WakeupException.class)
    public void shouldThrowWakeupExceptionOnInitializeOffsetsWhenWakeupException() throws Exception {
        final Consumer consumer = mockConsumer(new WakeupException());
        final AbstractTask task = createTask(consumer);
        task.updateOffsetLimits();
    }

    private AbstractTask createTask(final Consumer consumer) {
        final MockTime time = new MockTime();
        final Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummyhost:9092");
        final StreamsConfig config = new StreamsConfig(properties);
        return new AbstractTask(new TaskId(0, 0),
                                "app",
                                Collections.singletonList(new TopicPartition("t", 0)),
                                new ProcessorTopology(Collections.<ProcessorNode>emptyList(),
                                                      Collections.<String, SourceNode>emptyMap(),
                                                      Collections.<String, SinkNode>emptyMap(),
                                                      Collections.<StateStore>emptyList(),
                                                      Collections.<String, String>emptyMap(),
                                                      Collections.<StateStore>emptyList()),
                                consumer,
                                new StoreChangelogReader(consumer, Time.SYSTEM, 5000, new MockStateRestoreListener()),
                                false,
                                new StateDirectory("app", TestUtils.tempDirectory().getPath(), time),
                                config) {
            @Override
            public void resume() {}

            @Override
            public void commit() {}

            @Override
            public void suspend() {}

            @Override
            public void close(final boolean clean) {}
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
