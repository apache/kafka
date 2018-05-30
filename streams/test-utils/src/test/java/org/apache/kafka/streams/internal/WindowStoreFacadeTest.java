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
package org.apache.kafka.streams.internal;

import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.RecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

@SuppressWarnings("unchecked")
@RunWith(EasyMockRunner.class)
public class WindowStoreFacadeTest {
    @SuppressWarnings("UnusedDeclaration")
    @Mock(type = MockType.NICE)
    private WindowStore inner;
    @SuppressWarnings("UnusedDeclaration")
    @Mock(type = MockType.NICE)
    private InternalProcessorContext context;

    private WindowStoreFacade storeFacade;

    @Before
    public void before() {
        storeFacade = new WindowStoreFacade(inner, context);
    }

    @Test
    public void shouldInitContextAndCallPut() {
        inner.put("someKey", "someValue");
        context.setRecordContext(isA(RecordContext.class));
        context.setRecordContext(isNull());
        expectLastCall();
        replay(inner, context);

        storeFacade.put("someKey", "someValue");

        verify(inner, context);
    }

    @Test
    public void shouldInitContextAndCallPutWithTimestamp() {
        inner.put("someKey", "someValue", 42L);
        expectLastCall();
        context.setRecordContext(isA(RecordContext.class));
        context.setRecordContext(isNull());
        expectLastCall();
        replay(inner, context);

        storeFacade.put("someKey", "someValue", 42L);

        verify(inner, context);
    }

    @Test
    public void shouldNotInitContextAndFetchKeyTimestampFromInner() {
        inner.fetch("someKey", 42L);
        expectLastCall().andReturn("someValue");
        context.setRecordContext(anyObject());
        expectLastCall().andThrow(new AssertionError()).anyTimes();
        replay(inner, context);

        assertThat(storeFacade.fetch("someKey", 42L), equalTo("someValue"));
        verify(inner, context);
    }

    @Test
    public void shouldNotInitContextAndFetchKeyTimeRangeFromInner() {
        final WindowStoreIterator iterator = mock(WindowStoreIterator.class);

        inner.fetch("someKey", 21L, 42L);
        expectLastCall().andReturn(iterator);
        context.setRecordContext(anyObject());
        expectLastCall().andThrow(new AssertionError()).anyTimes();
        replay(inner, context);

        assertThat(storeFacade.fetch("someKey", 21L, 42L), equalTo(iterator));
        verify(inner, context);
    }

    @Test
    public void shouldNotInitContextAndFetchKeyRangeTimeRangeFromInner() {
        final KeyValueIterator iterator = mock(KeyValueIterator.class);

        inner.fetch("someKey", "someOtherKey", 21L, 42L);
        expectLastCall().andReturn(iterator);
        context.setRecordContext(anyObject());
        expectLastCall().andThrow(new AssertionError()).anyTimes();
        replay(inner, context);

        assertThat(storeFacade.fetch("someKey", "someOtherKey", 21L, 42L), equalTo(iterator));
        verify(inner, context);
    }

    @Test
    public void shouldNotInitContextAndGetAllFromInner() {
        final KeyValueIterator iterator = mock(KeyValueIterator.class);

        inner.all();
        expectLastCall().andReturn(iterator);
        context.setRecordContext(anyObject());
        expectLastCall().andThrow(new AssertionError()).anyTimes();
        replay(inner, context);

        assertThat(storeFacade.all(), equalTo(iterator));
        verify(inner, context);
    }

    @Test
    public void shouldNotInitContextAndFetchAllFromInner() {
        final KeyValueIterator iterator = mock(KeyValueIterator.class);

        inner.fetchAll(21L, 42L);
        expectLastCall().andReturn(iterator);
        context.setRecordContext(anyObject());
        expectLastCall().andThrow(new AssertionError()).anyTimes();
        replay(inner, context);

        assertThat(storeFacade.fetchAll(21L, 42L), equalTo(iterator));
        verify(inner, context);
    }
}
