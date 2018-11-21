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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProcessorContextImplTest {
    private ProcessorContextImpl context;

    private static final String KEY = "key";
    private static final long VAL = 42L;
    private static final String STORE_NAME = "underlying-store";

    private boolean initExecuted;
    private boolean closeExecuted;
    private KeyValueIterator<String, Long> rangeIter;
    private KeyValueIterator<String, Long> allIter;

    @Before
    public void setup() {
        final StreamsConfig streamsConfig = mock(StreamsConfig.class);
        expect(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).andReturn("add-id");
        expect(streamsConfig.defaultValueSerde()).andReturn(Serdes.ByteArray());
        expect(streamsConfig.defaultKeySerde()).andReturn(Serdes.ByteArray());
        replay(streamsConfig);

        rangeIter = mock(KeyValueIterator.class);
        allIter = mock(KeyValueIterator.class);

        final KeyValueStore<String, Long> globalStoreMock = mock(KeyValueStore.class);

        expect(globalStoreMock.get(KEY)).andReturn(VAL);
        expect(globalStoreMock.approximateNumEntries()).andReturn(VAL);
        expect(globalStoreMock.name()).andReturn(STORE_NAME);
        expect(globalStoreMock.persistent()).andReturn(true);
        expect(globalStoreMock.isOpen()).andReturn(true);

        expect(globalStoreMock.range("one", "two")).andReturn(rangeIter);
        expect(globalStoreMock.all()).andReturn(allIter);

        globalStoreMock.init(null, null);
        expectLastCall().andAnswer(() -> {
            initExecuted = true;
            return null;
        });

        globalStoreMock.close();
        expectLastCall().andAnswer(() -> {
            closeExecuted = true;
            return null;
        });

        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);

        expect(stateManager.getGlobalStore(anyString())).andReturn(globalStoreMock);

        replay(globalStoreMock, stateManager);

        context = new ProcessorContextImpl(
            mock(TaskId.class),
            mock(StreamTask.class),
            streamsConfig,
            mock(RecordCollector.class),
            stateManager,
            mock(StreamsMetricsImpl.class),
            mock(ThreadCache.class)
        );

        final Set<String> stateStores = new HashSet<>();

        stateStores.add("Counts");

        context.setCurrentNode(new ProcessorNode<String, Long>("fake", null, stateStores));
    }

    @Test
    public void testStateStoreWriteMethodThrows() {
        final Processor processor = new Processor<String, Long>() {
            @Override
            @SuppressWarnings("unchecked")
            public void init(final ProcessorContext context) {
                final KeyValueStore<String, Long> store = (KeyValueStore<String, Long>) context.getStateStore("Counts");

                checkThrowsUnsupportedOperation(() -> store.put("1", 1L), "put");
                checkThrowsUnsupportedOperation(() -> store.putIfAbsent("1", 1L), "putIfAbsent");
                checkThrowsUnsupportedOperation(() -> store.putAll(Collections.emptyList()), "putAll");
                checkThrowsUnsupportedOperation(() -> store.delete("1"), "delete");

                checkThrowsUnsupportedOperation(store::flush, "flush");

                assertEquals((Long) VAL, store.get(KEY));
                assertEquals(rangeIter, store.range("one", "two"));
                assertEquals(allIter, store.all());
                assertEquals(VAL, store.approximateNumEntries());
                assertEquals(STORE_NAME, store.name());
                assertTrue(store.persistent());
                assertTrue(store.isOpen());

                store.init(null, null);
                assertTrue(initExecuted);

                store.close();
                assertTrue(closeExecuted);
            }

            @Override
            public void process(final String k, final Long v) {
                //No-op.
            }

            @Override
            public void close() {
                //No-op.
            }
        };

        processor.init(context);

    }

    private void checkThrowsUnsupportedOperation(final Runnable check, final String name) {
        try {
            check.run();
            fail(name + " should throw exception");
        } catch (final UnsupportedOperationException e) {
            //ignore.
        }
    }
}
