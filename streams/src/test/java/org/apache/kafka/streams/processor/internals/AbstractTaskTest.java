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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.fail;

public class AbstractTaskTest {

    private final TaskId id = new TaskId(0, 0);
    private StateDirectory stateDirectory  = EasyMock.createMock(StateDirectory.class);

    @Before
    public void before() {
        EasyMock.expect(stateDirectory.directoryForTask(id)).andReturn(TestUtils.tempDirectory());
    }

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateExceptionOnInitializeOffsetsWhenAuthorizationException() {
        final Consumer consumer = mockConsumer(new AuthorizationException("blah"));
        final AbstractTask task = createTask(consumer, Collections.<StateStore>emptyList());
        task.updateOffsetLimits();
    }

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateExceptionOnInitializeOffsetsWhenKafkaException() {
        final Consumer consumer = mockConsumer(new KafkaException("blah"));
        final AbstractTask task = createTask(consumer, Collections.<StateStore>emptyList());
        task.updateOffsetLimits();
    }

    @Test(expected = WakeupException.class)
    public void shouldThrowWakeupExceptionOnInitializeOffsetsWhenWakeupException() {
        final Consumer consumer = mockConsumer(new WakeupException());
        final AbstractTask task = createTask(consumer, Collections.<StateStore>emptyList());
        task.updateOffsetLimits();
    }

    @Test
    public void shouldThrowLockExceptionIfFailedToLockStateDirectoryWhenTopologyHasStores() throws IOException {
        final Consumer consumer = EasyMock.createNiceMock(Consumer.class);
        final StateStore store = EasyMock.createNiceMock(StateStore.class);
        EasyMock.expect(stateDirectory.lock(id, 5)).andReturn(false);
        EasyMock.replay(stateDirectory);

        final AbstractTask task = createTask(consumer, Collections.singletonList(store));

        try {
            task.initializeStateStores();
            fail("Should have thrown LockException");
        } catch (final LockException e) {
            // ok
        }

    }

    @Test
    public void shouldNotAttemptToLockIfNoStores() throws IOException {
        final Consumer consumer = EasyMock.createNiceMock(Consumer.class);
        EasyMock.replay(stateDirectory);

        final AbstractTask task = createTask(consumer, Collections.<StateStore>emptyList());

        task.initializeStateStores();

        // should fail if lock is called
        EasyMock.verify(stateDirectory);
    }

    private AbstractTask createTask(final Consumer consumer, final List<StateStore> stateStores) {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummyhost:9092");
        final StreamsConfig config = new StreamsConfig(properties);
        return new AbstractTask(id,
                                "app",
                                Collections.singletonList(new TopicPartition("t", 0)),
                                new ProcessorTopology(Collections.<ProcessorNode>emptyList(),
                                                      Collections.<String, SourceNode>emptyMap(),
                                                      Collections.<String, SinkNode>emptyMap(),
                                                      stateStores,
                                                      Collections.<String, String>emptyMap(),
                                                      Collections.<StateStore>emptyList()),
                                consumer,
                                new StoreChangelogReader(consumer, new MockStateRestoreListener(), new LogContext("stream-task-test ")),
                                false,
                                stateDirectory,
                                config) {
            @Override
            public void resume() {}

            @Override
            public void commit() {}

            @Override
            public void suspend() {}

            @Override
            public void close(final boolean clean, final boolean isZombie) {}

            @Override
            public void closeSuspended(final boolean clean, final boolean isZombie, final RuntimeException e) {}

            @Override
            public Map<TopicPartition, Long> checkpointedOffsets() {
                return null;
            }

            @Override
            public boolean process() {
                return false;
            }

            @Override
            public boolean commitNeeded() {
                return false;
            }

            @Override
            public boolean maybePunctuateStreamTime() {
                return false;
            }

            @Override
            public boolean maybePunctuateSystemTime() {
                return false;
            }

            @Override
            public List<ConsumerRecord<byte[], byte[]>> update(final TopicPartition partition, final List<ConsumerRecord<byte[], byte[]>> remaining) {
                return null;
            }

            @Override
            public int addRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> records) {
                return 0;
            }

            @Override
            public boolean initialize() {
                return false;
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
