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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class GlobalProcessorContextImplTest {
    private static final String GLOBAL_STORE_NAME = "global-store";
    private static final String GLOBAL_KEY_VALUE_STORE_NAME = "global-key-value-store";
    private static final String GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME = "global-timestamped-key-value-store";
    private static final String GLOBAL_WINDOW_STORE_NAME = "global-window-store";
    private static final String GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME = "global-timestamped-window-store";
    private static final String GLOBAL_SESSION_STORE_NAME = "global-session-store";
    private static final String UNKNOWN_STORE = "unknown-store";

    private GlobalProcessorContextImpl globalContext;

    @Mock
    private ProcessorNode<Object, Object, Object, Object> child;
    private ProcessorRecordContext recordContext;
    @Mock
    private GlobalStateManager stateManager;

    @Before
    public void setup() {
        final StreamsConfig streamsConfig = mock(StreamsConfig.class);
        when(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).thenReturn("dummy-id");

        globalContext = new GlobalProcessorContextImpl(
            streamsConfig,
            stateManager,
            null,
            null,
            Time.SYSTEM);

        final ProcessorNode<Object, Object, Object, Object> processorNode = new ProcessorNode<>("testNode");

        processorNode.addChild(child);

        globalContext.setCurrentNode(processorNode);
        recordContext = mock(ProcessorRecordContext.class);
        globalContext.setRecordContext(recordContext);
    }

    @Test
    public void shouldReturnGlobalOrNullStore() {
        when(stateManager.getGlobalStore(GLOBAL_STORE_NAME)).thenReturn(mock(StateStore.class));
        assertThat(globalContext.getStateStore(GLOBAL_STORE_NAME), new IsInstanceOf(StateStore.class));
        assertNull(globalContext.getStateStore(UNKNOWN_STORE));
    }

    @Test
    public void shouldForwardToSingleChild() {
        doNothing().when(child).process(any());

        when(recordContext.timestamp()).thenReturn(0L);
        when(recordContext.headers()).thenReturn(new RecordHeaders());
        globalContext.forward((Object /*forcing a call to the K/V forward*/) null, null);
    }

    @Test
    public void shouldFailToForwardUsingToParameter() {
        assertThrows(IllegalStateException.class, () -> globalContext.forward(null, null, To.all()));
    }

    @Test
    public void shouldNotFailOnNoOpCommit() {
        globalContext.commit();
    }

    @Test
    public void shouldNotAllowToSchedulePunctuations() {
        assertThrows(UnsupportedOperationException.class, () -> globalContext.schedule(null, null, null));
    }

    @Test
    public void shouldNotAllowInitForKeyValueStore() {
        when(stateManager.getGlobalStore(GLOBAL_KEY_VALUE_STORE_NAME)).thenReturn(mock(KeyValueStore.class));
        final StateStore store = globalContext.getStateStore(GLOBAL_KEY_VALUE_STORE_NAME);
        try {
            store.init((StateStoreContext) null, null);
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowInitForTimestampedKeyValueStore() {
        when(stateManager.getGlobalStore(GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME)).thenReturn(mock(TimestampedKeyValueStore.class));
        final StateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME);
        try {
            store.init((StateStoreContext) null, null);
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowInitForWindowStore() {
        when(stateManager.getGlobalStore(GLOBAL_WINDOW_STORE_NAME)).thenReturn(mock(WindowStore.class));
        final StateStore store = globalContext.getStateStore(GLOBAL_WINDOW_STORE_NAME);
        try {
            store.init((StateStoreContext) null, null);
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowInitForTimestampedWindowStore() {
        when(stateManager.getGlobalStore(GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME)).thenReturn(mock(TimestampedWindowStore.class));
        final StateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME);
        try {
            store.init((StateStoreContext) null, null);
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowInitForSessionStore() {
        when(stateManager.getGlobalStore(GLOBAL_SESSION_STORE_NAME)).thenReturn(mock(SessionStore.class));
        final StateStore store = globalContext.getStateStore(GLOBAL_SESSION_STORE_NAME);
        try {
            store.init((StateStoreContext) null, null);
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowCloseForKeyValueStore() {
        when(stateManager.getGlobalStore(GLOBAL_KEY_VALUE_STORE_NAME)).thenReturn(mock(KeyValueStore.class));
        final StateStore store = globalContext.getStateStore(GLOBAL_KEY_VALUE_STORE_NAME);
        try {
            store.close();
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowCloseForTimestampedKeyValueStore() {
        when(stateManager.getGlobalStore(GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME)).thenReturn(mock(TimestampedKeyValueStore.class));
        final StateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME);
        try {
            store.close();
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowCloseForWindowStore() {
        when(stateManager.getGlobalStore(GLOBAL_WINDOW_STORE_NAME)).thenReturn(mock(WindowStore.class));
        final StateStore store = globalContext.getStateStore(GLOBAL_WINDOW_STORE_NAME);
        try {
            store.close();
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowCloseForTimestampedWindowStore() {
        when(stateManager.getGlobalStore(GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME)).thenReturn(mock(TimestampedWindowStore.class));
        final StateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME);
        try {
            store.close();
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowCloseForSessionStore() {
        when(stateManager.getGlobalStore(GLOBAL_SESSION_STORE_NAME)).thenReturn(mock(SessionStore.class));
        final StateStore store = globalContext.getStateStore(GLOBAL_SESSION_STORE_NAME);
        try {
            store.close();
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldThrowOnCurrentStreamTime() {
        assertThrows(UnsupportedOperationException.class, () -> globalContext.currentStreamTimeMs());
    }
}