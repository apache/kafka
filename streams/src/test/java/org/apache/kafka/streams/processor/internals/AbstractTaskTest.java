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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.ProcessorTopologyFactories.withLocalStores;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.fail;

public class AbstractTaskTest {

    private final TaskId id = new TaskId(0, 0);
    private StateDirectory stateDirectory  = EasyMock.createMock(StateDirectory.class);
    private final TopicPartition storeTopicPartition1 = new TopicPartition("t1", 0);
    private final TopicPartition storeTopicPartition2 = new TopicPartition("t2", 0);
    private final TopicPartition storeTopicPartition3 = new TopicPartition("t3", 0);
    private final TopicPartition storeTopicPartition4 = new TopicPartition("t4", 0);
    private final Set<TopicPartition> storeTopicPartitions =
        Utils.mkSet(storeTopicPartition1, storeTopicPartition2, storeTopicPartition3, storeTopicPartition4);

    @Before
    public void before() {
        expect(stateDirectory.directoryForTask(id)).andReturn(TestUtils.tempDirectory());
    }

    @Test
    public void shouldThrowLockExceptionIfFailedToLockStateDirectoryWhenTopologyHasStores() throws IOException {
        final Consumer consumer = EasyMock.createNiceMock(Consumer.class);
        final StateStore store = EasyMock.createNiceMock(StateStore.class);
        expect(store.name()).andReturn("dummy-store-name").anyTimes();
        EasyMock.replay(store);
        expect(stateDirectory.lock(id)).andReturn(false);
        EasyMock.replay(stateDirectory);

        final AbstractTask task = createTask(consumer, Collections.singletonMap(store, "dummy"));

        try {
            task.registerStateStores();
            fail("Should have thrown LockException");
        } catch (final LockException e) {
            // ok
        }

    }

    @Test
    public void shouldNotAttemptToLockIfNoStores() {
        final Consumer consumer = EasyMock.createNiceMock(Consumer.class);
        EasyMock.replay(stateDirectory);

        final AbstractTask task = createTask(consumer, Collections.emptyMap());

        task.registerStateStores();

        // should fail if lock is called
        EasyMock.verify(stateDirectory);
    }

    private AbstractTask createTask(final Consumer consumer,
                                    final Map<StateStore, String> stateStoresToChangelogTopics) {
        return createTask(consumer, stateStoresToChangelogTopics, stateDirectory);
    }

    @SuppressWarnings("unchecked")
    private AbstractTask createTask(final Consumer consumer,
                                    final Map<StateStore, String> stateStoresToChangelogTopics,
                                    final StateDirectory stateDirectory) {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummyhost:9092");
        final StreamsConfig config = new StreamsConfig(properties);

        final Map<String, String> storeNamesToChangelogTopics = new HashMap<>(stateStoresToChangelogTopics.size());
        for (final Map.Entry<StateStore, String> e : stateStoresToChangelogTopics.entrySet()) {
            storeNamesToChangelogTopics.put(e.getKey().name(), e.getValue());
        }

        final LogContext logContext = new LogContext("stream-task-test ");
        final StoreChangelogReader changelogReader = new StoreChangelogReader(config, logContext, consumer, new MockStateRestoreListener());
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            id,
            storeTopicPartitions,
            AbstractTask.TaskType.ACTIVE,
            stateDirectory,
            storeNamesToChangelogTopics,
            changelogReader,
            logContext);

//        return new AbstractTask(id,
//                                storeTopicPartitions,
//                                withLocalStores(new ArrayList<>(stateStoresToChangelogTopics.keySet()),
//                                                storeNamesToChangelogTopics),
//                                consumer,
//                                false,
//                                stateManager,
//                                stateDirectory,
//                                config) {
//
//
//            private State state = State.CREATED;
//
//            @Override
//            public State state() {
//                return state;
//            }
//
//            @Override
//            public void transitionTo(final State newState) {
//                State.validateTransition(state, newState);
//                state = newState;
//            }
//
//            @Override
//            public void initializeMetadata() {}
//
//            @Override
//            public void resume() {}
//
//            @Override
//            public void commit() {}
//
//            @Override
//            public void close(final boolean clean) {}
//
//            @Override
//            public boolean initializeStateStores() {
//                return false;
//            }
//
//            @Override
//            public void initializeTopology() {}
//        };
        throw new RuntimeException();
    }
}
