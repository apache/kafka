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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class GlobalProcessorContextImplTest {
    private static final String GLOBAL_STORE_NAME = "global-store";
    private static final String GLOBAL_KEY_VALUE_STORE_NAME = "global-key-value-store";
    private static final String GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME = "global-timestamped-key-value-store";
    private static final String GLOBAL_WINDOW_STORE_NAME = "global-window-store";
    private static final String GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME = "global-timestamped-window-store";
    private static final String GLOBAL_SESSION_STORE_NAME = "global-session-store";
    private static final String UNKNOWN_STORE = "unknown-store";
    private static final String CHILD_PROCESSOR = "child";

    private GlobalProcessorContextImpl globalContext;

    private ProcessorNode<Object, Object, Object, Object> child;
    private ProcessorRecordContext recordContext;

    @Before
    public void setup() {
        final StreamsConfig streamsConfig = mock(StreamsConfig.class);
        expect(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).andReturn("dummy-id");
        expect(streamsConfig.defaultValueSerde()).andReturn(Serdes.ByteArray());
        expect(streamsConfig.defaultKeySerde()).andReturn(Serdes.ByteArray());
        replay(streamsConfig);

        final GlobalStateManager stateManager = mock(GlobalStateManager.class);
        expect(stateManager.getGlobalStore(GLOBAL_STORE_NAME)).andReturn(mock(StateStore.class));
        expect(stateManager.getGlobalStore(GLOBAL_KEY_VALUE_STORE_NAME)).andReturn(mock(KeyValueStore.class));
        expect(stateManager.getGlobalStore(GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME)).andReturn(mock(TimestampedKeyValueStore.class));
        expect(stateManager.getGlobalStore(GLOBAL_WINDOW_STORE_NAME)).andReturn(mock(WindowStore.class));
        expect(stateManager.getGlobalStore(GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME)).andReturn(mock(TimestampedWindowStore.class));
        expect(stateManager.getGlobalStore(GLOBAL_SESSION_STORE_NAME)).andReturn(mock(SessionStore.class));
        expect(stateManager.getGlobalStore(UNKNOWN_STORE)).andReturn(null);
        expect(stateManager.taskType()).andStubReturn(TaskType.GLOBAL);
        replay(stateManager);

        globalContext = new GlobalProcessorContextImpl(
            streamsConfig,
            stateManager,
            null,
            null);

        final ProcessorNode<Object, Object, Object, Object> processorNode = new ProcessorNode<>("testNode");

        child = mock(ProcessorNode.class);
        processorNode.addChild(child);

        globalContext.setCurrentNode(processorNode);
        recordContext = mock(ProcessorRecordContext.class);
        globalContext.setRecordContext(recordContext);
    }

    @Test
    public void shouldReturnGlobalOrNullStore() {
        assertThat(globalContext.getStateStore(GLOBAL_STORE_NAME), new IsInstanceOf(StateStore.class));
        assertNull(globalContext.getStateStore(UNKNOWN_STORE));
    }

    @Test
    public void shouldForwardToSingleChild() {
        child.process(anyObject());
        expectLastCall();

        expect(recordContext.timestamp()).andStubReturn(0L);
        expect(recordContext.headers()).andStubReturn(new RecordHeaders());
        replay(child, recordContext);
        globalContext.forward((Object /*forcing a call to the K/V forward*/) null, null);
        verify(child, recordContext);
    }

    @Test
    public void shouldFailToForwardUsingToParameter() {
        assertThrows(IllegalStateException.class, () -> globalContext.forward(null, null, To.all()));
    }

    @SuppressWarnings("deprecation") // need to test deprecated code until removed
    @Test
    public void shouldNotSupportForwardingViaChildIndex() {
        assertThrows(UnsupportedOperationException.class, () -> globalContext.forward(null, null, 0));
    }

    @SuppressWarnings("deprecation") // need to test deprecated code until removed
    @Test
    public void shouldNotSupportForwardingViaChildName() {
        assertThrows(UnsupportedOperationException.class, () -> globalContext.forward(null, null, "processorName"));
    }

    @Test
    public void shouldNotFailOnNoOpCommit() {
        globalContext.commit();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldNotAllowToSchedulePunctuationsUsingDeprecatedApi() {
        assertThrows(UnsupportedOperationException.class, () -> globalContext.schedule(0L, null, null));
    }

    @Test
    public void shouldNotAllowToSchedulePunctuations() {
        assertThrows(UnsupportedOperationException.class, () -> globalContext.schedule(null, null, null));
    }

    @Test
    public void shouldNotAllowInitForKeyValueStore() {
        final StateStore store = globalContext.getStateStore(GLOBAL_KEY_VALUE_STORE_NAME);
        try {
            store.init((StateStoreContext) null, null);
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowInitForTimestampedKeyValueStore() {
        final StateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME);
        try {
            store.init((StateStoreContext) null, null);
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowInitForWindowStore() {
        final StateStore store = globalContext.getStateStore(GLOBAL_WINDOW_STORE_NAME);
        try {
            store.init((StateStoreContext) null, null);
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowInitForTimestampedWindowStore() {
        final StateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME);
        try {
            store.init((StateStoreContext) null, null);
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowInitForSessionStore() {
        final StateStore store = globalContext.getStateStore(GLOBAL_SESSION_STORE_NAME);
        try {
            store.init((StateStoreContext) null, null);
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowCloseForKeyValueStore() {
        final StateStore store = globalContext.getStateStore(GLOBAL_KEY_VALUE_STORE_NAME);
        try {
            store.close();
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowCloseForTimestampedKeyValueStore() {
        final StateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME);
        try {
            store.close();
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowCloseForWindowStore() {
        final StateStore store = globalContext.getStateStore(GLOBAL_WINDOW_STORE_NAME);
        try {
            store.close();
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowCloseForTimestampedWindowStore() {
        final StateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME);
        try {
            store.close();
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }

    @Test
    public void shouldNotAllowCloseForSessionStore() {
        final StateStore store = globalContext.getStateStore(GLOBAL_SESSION_STORE_NAME);
        try {
            store.close();
            fail("Should have thrown UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) { }
    }
}