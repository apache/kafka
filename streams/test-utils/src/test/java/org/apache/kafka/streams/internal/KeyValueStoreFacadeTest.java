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
import org.apache.kafka.streams.state.KeyValueStore;
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
public class KeyValueStoreFacadeTest {
    @SuppressWarnings("UnusedDeclaration")
    @Mock(type = MockType.NICE)
    private KeyValueStore inner;
    @SuppressWarnings("UnusedDeclaration")
    @Mock(type = MockType.NICE)
    private InternalProcessorContext context;

    private KeyValueStoreFacade storeFacade;

    @Before
    public void before() {
        storeFacade = new KeyValueStoreFacade(inner, context);
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
    public void shouldInitContextAndCallPutIfAbsent() {
        inner.putIfAbsent("someKey", "someValue");
        expectLastCall().andReturn("someOldValue");
        context.setRecordContext(isA(RecordContext.class));
        context.setRecordContext(isNull());
        expectLastCall();
        replay(inner, context);

        assertThat(storeFacade.putIfAbsent("someKey", "someValue"), equalTo("someOldValue"));
        verify(inner, context);
    }

    @Test
    public void shouldInitContextAndCallDelete() {
        inner.delete("someKey");
        expectLastCall().andReturn("someValue");
        context.setRecordContext(isA(RecordContext.class));
        context.setRecordContext(isNull());
        expectLastCall();
        replay(inner, context);

        assertThat(storeFacade.delete("someKey"), equalTo("someValue"));
        verify(inner, context);
    }

    @Test
    public void shouldNotInitContextAndGetFromInner() {
        inner.get("someKey");
        expectLastCall().andReturn("someValue");
        context.setRecordContext(anyObject());
        expectLastCall().andThrow(new AssertionError()).anyTimes();
        replay(inner, context);

        assertThat(storeFacade.get("someKey"), equalTo("someValue"));
        verify(inner, context);
    }

    @Test
    public void shouldNotInitContextAndRangeOnInner() {
        final KeyValueIterator iterator = mock(KeyValueIterator.class);

        inner.range("someKey", "someOtherKey");
        expectLastCall().andReturn(iterator);
        context.setRecordContext(anyObject());
        expectLastCall().andThrow(new AssertionError()).anyTimes();
        replay(inner, context);

        assertThat(storeFacade.range("someKey", "someOtherKey"), equalTo(iterator));
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
    public void shouldNotInitContextAndGetApproximateFromInner() {
        inner.approximateNumEntries();
        expectLastCall().andReturn(42L);
        context.setRecordContext(anyObject());
        expectLastCall().andThrow(new AssertionError()).anyTimes();
        replay(inner, context);

        assertThat(storeFacade.approximateNumEntries(), equalTo(42L));
        verify(inner, context);
    }
}
