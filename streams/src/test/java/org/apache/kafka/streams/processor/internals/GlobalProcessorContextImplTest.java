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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class GlobalProcessorContextImplTest {
    private static final String GLOBAL_STORE_NAME = "global-store";
    private static final String UNKNOWN_STORE = "unknown-store";
    private static final String CHILD_PROCESSOR = "child";

    private GlobalProcessorContextImpl globalContext;

    private ProcessorNode child;
    private ProcessorRecordContext recordContext;

    @Before
    public void setup() {
        final StreamsConfig streamsConfig = mock(StreamsConfig.class);
        expect(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).andReturn("dummy-id");
        expect(streamsConfig.defaultValueSerde()).andReturn(Serdes.ByteArray());
        expect(streamsConfig.defaultKeySerde()).andReturn(Serdes.ByteArray());
        replay(streamsConfig);

        final StateManager stateManager = mock(StateManager.class);
        expect(stateManager.getGlobalStore(GLOBAL_STORE_NAME)).andReturn(mock(KeyValueStore.class));
        expect(stateManager.getGlobalStore(UNKNOWN_STORE)).andReturn(null);
        replay(stateManager);

        globalContext = new GlobalProcessorContextImpl(
            streamsConfig,
            stateManager,
            null,
            null);

        final ProcessorNode processorNode = mock(ProcessorNode.class);
        globalContext.setCurrentNode(processorNode);

        child = mock(ProcessorNode.class);

        expect(processorNode.children())
            .andReturn(Collections.singletonList(child))
            .anyTimes();
        expect(processorNode.getChild(CHILD_PROCESSOR))
            .andReturn(child);
        expect(processorNode.getChild(anyString()))
            .andReturn(null);
        replay(processorNode);

        recordContext = mock(ProcessorRecordContext.class);
        globalContext.setRecordContext(recordContext);
    }

    @Test
    public void shouldReturnGlobalOrNullStore() {
        assertThat(globalContext.getStateStore(GLOBAL_STORE_NAME), new IsInstanceOf(KeyValueStore.class));
        assertNull(globalContext.getStateStore(UNKNOWN_STORE));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldForwardToSingleChild() {
        child.process(null, null);
        expectLastCall();

        replay(child, recordContext);
        globalContext.forward(null, null);
        verify(child, recordContext);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFailToForwardUsingToParameter() {
        globalContext.forward(null, null, To.all());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotSupportForwardingViaChildIndex() {
        globalContext.forward(null, null, 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotSupportForwardingViaChildName() {
        globalContext.forward(null, null, "processorName");
    }

    @Test
    public void shouldNotFailOnNoOpCommit() {
        globalContext.commit();
    }

    @SuppressWarnings("deprecation")
    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowToSchedulePunctuationsUsingDeprecatedApi() {
        globalContext.schedule(0L, null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowToSchedulePunctuations() {
        globalContext.schedule(null, null, null);
    }
}
