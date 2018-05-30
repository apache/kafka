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

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.RecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
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
public class SessionStoreFacadeTest {
    @SuppressWarnings("UnusedDeclaration")
    @Mock(type = MockType.NICE)
    private SessionStore inner;
    @SuppressWarnings("UnusedDeclaration")
    @Mock(type = MockType.NICE)
    private InternalProcessorContext context;

    private SessionStoreFacade storeFacade;

    @Before
    public void before() {
        storeFacade = new SessionStoreFacade(inner, context);
    }

    @Test
    public void shouldInitContextAndCallPut() {
        inner.put(new Windowed("someKey", new SessionWindow(21L, 42L)), "someAggregate");
        context.setRecordContext(isA(RecordContext.class));
        context.setRecordContext(isNull());
        expectLastCall();
        replay(inner, context);

        storeFacade.put(new Windowed("someKey", new SessionWindow(21L, 42L)), "someAggregate");

        verify(inner, context);
    }

    @Test
    public void shouldInitContextAndCallRemove() {
        inner.remove(new Windowed("someKey", new SessionWindow(21L, 42L)));
        context.setRecordContext(isA(RecordContext.class));
        context.setRecordContext(isNull());
        expectLastCall();
        replay(inner, context);

        storeFacade.remove(new Windowed("someKey", new SessionWindow(21L, 42L)));

        verify(inner, context);
    }

    @Test
    public void shouldNotInitContextAndFindSessionForKey() {
        final KeyValueIterator iterator = mock(KeyValueIterator.class);

        inner.findSessions("someKey", 21L, 42L);
        expectLastCall().andReturn(iterator);
        context.setRecordContext(anyObject());
        expectLastCall().andThrow(new AssertionError()).anyTimes();
        replay(inner, context);

        assertThat(storeFacade.findSessions("someKey", 21L, 42L), equalTo(iterator));

        verify(inner, context);
    }

    @Test
    public void shouldNotInitContextAndFindSessionForKeyRange() {
        final KeyValueIterator iterator = mock(KeyValueIterator.class);

        inner.findSessions("someKey", "someOtherKey", 21L, 42L);
        expectLastCall().andReturn(iterator);
        context.setRecordContext(anyObject());
        expectLastCall().andThrow(new AssertionError()).anyTimes();
        replay(inner, context);

        assertThat(storeFacade.findSessions("someKey", "someOtherKey", 21L, 42L), equalTo(iterator));

        verify(inner, context);
    }

    @Test
    public void shouldNotInitContextAndFetchForKey() {
        final KeyValueIterator iterator = mock(KeyValueIterator.class);

        inner.fetch("someKey");
        expectLastCall().andReturn(iterator);
        context.setRecordContext(anyObject());
        expectLastCall().andThrow(new AssertionError()).anyTimes();
        replay(inner, context);

        assertThat(storeFacade.fetch("someKey"), equalTo(iterator));

        verify(inner, context);
    }

    @Test
    public void shouldNotInitContextAndFetchForKeyRange() {
        final KeyValueIterator iterator = mock(KeyValueIterator.class);

        inner.fetch("someKey", "someOtherKey");
        expectLastCall().andReturn(iterator);
        context.setRecordContext(anyObject());
        expectLastCall().andThrow(new AssertionError()).anyTimes();
        replay(inner, context);

        assertThat(storeFacade.fetch("someKey", "someOtherKey"), equalTo(iterator));

        verify(inner, context);
    }
}
