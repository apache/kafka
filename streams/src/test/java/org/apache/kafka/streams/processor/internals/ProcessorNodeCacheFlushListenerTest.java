/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.MockProcessorNode;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ProcessorNodeCacheFlushListenerTest {

    private MockProcessorNode<String, String> parent;
    private List<TheProcessorNode> children;
    private MockProcessorContext context;
    private ProcessorNodeCacheFlushListener<String, String> listener;
    private ProcessorRecordContextImpl initialRecordContext;

    @Before
    public void setUp() throws Exception {
        parent = new MockProcessorNode<>(-1);
        children = Arrays.asList(new TheProcessorNode("one"),
                                 new TheProcessorNode("two"));
        context = new MockProcessorContext(StateSerdes.withBuiltinTypes("state", String.class, String.class), null);
        initialRecordContext = new ProcessorRecordContextImpl(-1, -1, -1, "", parent, false);
        context.setRecordContext(initialRecordContext);
        for (TheProcessorNode child : children) {
            parent.addChild(child);
            child.init(context);
        }
        listener = new ProcessorNodeCacheFlushListener<>(parent);
    }

    @Test
    public void shouldForwardToChildrenWithCorrectContext() throws Exception {
        listener.forward("key", new Change<>("value", null), new RecordContextStub(1, 1, 1, "topic"), context);

        assertEquals(new ProcessorRecordContextImpl(1, 1, 1, "topic", children.get(0), false), children.get(0).recordContexts.get("key"));
        assertEquals(new ProcessorRecordContextImpl(1, 1, 1, "topic", children.get(1), false), children.get(1).recordContexts.get("key"));
        assertEquals(1, children.get(0).recordContexts.size());
        assertEquals(1, children.get(1).recordContexts.size());
    }

    @Test
    public void shouldResetContextToOriginalOnceComplete() throws Exception {
        listener.forward("bar", new Change<String>("foo", null), new RecordContextStub(1, 1, 1, "topic"), context);
        assertEquals(initialRecordContext, context.processorRecordContext());
    }

    static class TheProcessorNode extends ProcessorNode<String, Change<String>> {

        private ProcessorContext context;
        private Map<String, RecordContext> recordContexts = new HashMap<>();

        public TheProcessorNode(final String name) {
            super(name);
        }

        @Override
        public void init(final ProcessorContext context) {
            this.context = context;
        }

        @Override
        public void process(final String key, final Change<String> value) {
            recordContexts.put(key, context.processorRecordContext());
        }

    }

}