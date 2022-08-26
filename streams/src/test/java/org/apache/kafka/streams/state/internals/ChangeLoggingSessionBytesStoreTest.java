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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.test.MockRecordCollector;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Optional;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

@RunWith(EasyMockRunner.class)
public class ChangeLoggingSessionBytesStoreTest {

    private final TaskId taskId = new TaskId(0, 0);
    private final MockRecordCollector collector = new MockRecordCollector();

    @Mock(type = MockType.NICE)
    private SessionStore<Bytes, byte[]> inner;
    @Mock(type = MockType.NICE)
    private ProcessorContextImpl context;

    private ChangeLoggingSessionBytesStore store;
    private final byte[] value1 = {0};
    private final Bytes bytesKey = Bytes.wrap(value1);
    private final Windowed<Bytes> key1 = new Windowed<>(bytesKey, new SessionWindow(0, 0));

    private final static Position POSITION = Position.fromMap(mkMap(mkEntry("", mkMap(mkEntry(0, 1L)))));

    @Before
    public void setUp() {
        store = new ChangeLoggingSessionBytesStore(inner);
    }

    private void init() {
        EasyMock.expect(context.taskId()).andReturn(taskId).anyTimes();
        EasyMock.expect(context.recordCollector()).andReturn(collector).anyTimes();
        EasyMock.expect(context.recordMetadata()).andReturn(Optional.empty()).anyTimes();
        inner.init((StateStoreContext) context, store);
        EasyMock.expectLastCall();
        EasyMock.replay(inner, context);
        store.init((StateStoreContext) context, store);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldDelegateDeprecatedInit() {
        inner.init((ProcessorContext) context, store);
        EasyMock.expectLastCall();
        EasyMock.replay(inner);
        store.init((ProcessorContext) context, store);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldDelegateInit() {
        inner.init((StateStoreContext) context, store);
        EasyMock.expectLastCall();
        EasyMock.replay(inner);
        store.init((StateStoreContext) context, store);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldLogPuts() {
        EasyMock.expect(inner.getPosition()).andReturn(Position.emptyPosition()).anyTimes();
        inner.put(key1, value1);
        EasyMock.expectLastCall();

        init();

        final Bytes binaryKey = SessionKeySchema.toBinary(key1);

        EasyMock.reset(context);
        EasyMock.expect(context.recordMetadata()).andStubReturn(Optional.empty());
        context.logChange(store.name(), binaryKey, value1, 0L, Position.emptyPosition());

        EasyMock.replay(context);
        store.put(key1, value1);

        EasyMock.verify(inner, context);
    }

    @Test
    public void shouldLogPutsWithPosition() {
        EasyMock.expect(inner.getPosition()).andReturn(POSITION).anyTimes();
        inner.put(key1, value1);
        EasyMock.expectLastCall();

        init();

        final Bytes binaryKey = SessionKeySchema.toBinary(key1);

        EasyMock.reset(context);
        final RecordMetadata recordContext = new ProcessorRecordContext(0L, 1L, 0, "", new RecordHeaders());
        EasyMock.expect(context.recordMetadata()).andStubReturn(Optional.of(recordContext));
        EasyMock.expect(context.timestamp()).andStubReturn(0L);
        context.logChange(store.name(), binaryKey, value1, 0L, POSITION);

        EasyMock.replay(context);
        store.put(key1, value1);

        EasyMock.verify(inner, context);
    }

    @Test
    public void shouldLogRemoves() {
        EasyMock.expect(inner.getPosition()).andReturn(Position.emptyPosition()).anyTimes();
        inner.remove(key1);
        EasyMock.expectLastCall();

        init();
        store.remove(key1);

        final Bytes binaryKey = SessionKeySchema.toBinary(key1);

        EasyMock.reset(context);
        EasyMock.expect(context.recordMetadata()).andStubReturn(Optional.empty());
        context.logChange(store.name(), binaryKey, null, 0L, Position.emptyPosition());

        EasyMock.replay(context);
        store.remove(key1);

        EasyMock.verify(inner, context);
    }

    @Test
    public void shouldDelegateToUnderlyingStoreWhenFetching() {
        EasyMock.expect(inner.fetch(bytesKey)).andReturn(KeyValueIterators.emptyIterator());

        init();

        store.fetch(bytesKey);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldDelegateToUnderlyingStoreWhenBackwardFetching() {
        EasyMock.expect(inner.backwardFetch(bytesKey)).andReturn(KeyValueIterators.emptyIterator());

        init();

        store.backwardFetch(bytesKey);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldDelegateToUnderlyingStoreWhenFetchingRange() {
        EasyMock.expect(inner.fetch(bytesKey, bytesKey)).andReturn(KeyValueIterators.emptyIterator());

        init();

        store.fetch(bytesKey, bytesKey);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldDelegateToUnderlyingStoreWhenBackwardFetchingRange() {
        EasyMock.expect(inner.backwardFetch(bytesKey, bytesKey)).andReturn(KeyValueIterators.emptyIterator());

        init();

        store.backwardFetch(bytesKey, bytesKey);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldDelegateToUnderlyingStoreWhenFindingSessions() {
        EasyMock.expect(inner.findSessions(bytesKey, 0, 1)).andReturn(KeyValueIterators.emptyIterator());

        init();

        store.findSessions(bytesKey, 0, 1);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldDelegateToUnderlyingStoreWhenBackwardFindingSessions() {
        EasyMock.expect(inner.backwardFindSessions(bytesKey, 0, 1)).andReturn(KeyValueIterators.emptyIterator());

        init();

        store.backwardFindSessions(bytesKey, 0, 1);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldDelegateToUnderlyingStoreWhenFindingSessionRange() {
        EasyMock.expect(inner.findSessions(bytesKey, bytesKey, 0, 1)).andReturn(KeyValueIterators.emptyIterator());

        init();

        store.findSessions(bytesKey, bytesKey, 0, 1);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldDelegateToUnderlyingStoreWhenBackwardFindingSessionRange() {
        EasyMock.expect(inner.backwardFindSessions(bytesKey, bytesKey, 0, 1)).andReturn(KeyValueIterators.emptyIterator());

        init();

        store.backwardFindSessions(bytesKey, bytesKey, 0, 1);
        EasyMock.verify(inner);
    }

    @Test
    public void shouldFlushUnderlyingStore() {
        inner.flush();
        EasyMock.expectLastCall();

        init();

        store.flush();
        EasyMock.verify(inner);
    }

    @Test
    public void shouldCloseUnderlyingStore() {
        inner.close();
        EasyMock.expectLastCall();

        init();

        store.close();
        EasyMock.verify(inner);
    }
}