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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CachingKeyValueStoreQueryTest {
    private static final KeyQuery<String, Integer> QUERY = KeyQuery.withKey("key");
    private static final QueryConfig READ_COMMITTED = new QueryConfig(false, IsolationLevel.READ_COMMITTED);
    private static final QueryConfig READ_UNCOMMITTED = new QueryConfig(false, IsolationLevel.READ_UNCOMMITTED);
    private KeyValueStore<String, Integer> store;

    @BeforeEach
    public void setUp() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG, true);
        final ThreadCache cache = new ThreadCache(new LogContext("test "), 1_000_000, new MockStreamsMetrics(new Metrics()));
        final InternalMockProcessorContext<?, ?> context = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(), Serdes.String(), Serdes.Integer(), null, cache, new StreamsConfig(props));
        store = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("store"), Serdes.String(), Serdes.Integer())
            .withCachingEnabled()
            .withLoggingDisabled()
            .build();
        store.init(context, store);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "topic", new RecordHeaders()));
        store.put("key", 10);
        store.commit(Map.of());
        context.setRecordContext(new ProcessorRecordContext(0, 1, 0, "topic", new RecordHeaders()));
        store.put("key", 20);
    }

    @AfterEach
    public void tearDown() {
        store.close();
    }

    @Test
    public void shouldHideCachedUpdateFromReadCommittedQueryUntilCommit() {
        final PositionBound unbounded = PositionBound.unbounded();
        assertEquals(10, store.query(QUERY, unbounded, READ_COMMITTED).getResult());
        assertEquals(20, store.query(QUERY, unbounded, READ_UNCOMMITTED).getResult());

        ((CachedStateStore<?, ?>) store).flushCache();
        assertEquals(10, store.query(QUERY, unbounded, READ_COMMITTED).getResult());
        assertEquals(20, store.query(QUERY, unbounded, READ_UNCOMMITTED).getResult());

        store.commit(Map.of());
        assertEquals(20, store.query(QUERY, unbounded, READ_COMMITTED).getResult());
        assertEquals(20, store.query(QUERY, unbounded, READ_UNCOMMITTED).getResult());
    }

    @Test
    public void shouldCheckReadCommittedQueryBoundAgainstCommittedPosition() {
        final Position committed = Position.emptyPosition().withComponent("topic", 0, 0);
        final Position pending = Position.emptyPosition().withComponent("topic", 0, 1);
        final PositionBound bound = PositionBound.at(pending);

        final QueryResult<Integer> beforeCommit = store.query(QUERY, bound, READ_COMMITTED);
        assertEquals(FailureReason.NOT_UP_TO_BOUND, beforeCommit.getFailureReason());
        assertEquals(committed, beforeCommit.getPosition());
        assertEquals(pending, store.query(QUERY, bound, READ_UNCOMMITTED).getPosition());

        store.commit(Map.of());
        final QueryResult<Integer> afterCommit = store.query(QUERY, bound, READ_COMMITTED);
        assertEquals(20, afterCommit.getResult());
        assertEquals(pending, afterCommit.getPosition());
    }
}
