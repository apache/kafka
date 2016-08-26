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

import org.apache.kafka.test.MockProcessorNode;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class ProcessorRecordContextImplTest {

    private ProcessorRecordContextImpl processorRecordContext;
    private MockProcessorNode parentNode;

    @Before
    public void setUp() throws Exception {
        parentNode = new MockProcessorNode(-1L);
        processorRecordContext = new ProcessorRecordContextImpl(0, 0, 0, "t", parentNode);
    }

    @Test
    public void shouldForwardToAllChildren() throws Exception {
        final MockProcessorNode<Object, Object> first = new MockProcessorNode<>(-1);
        final MockProcessorNode<Object, Object> second = new MockProcessorNode<>(-1);
        parentNode.addChild(first);
        parentNode.addChild(second);
        processorRecordContext.forward("key", "value");
        assertEquals(1, first.numReceived);
        assertEquals(1, second.numReceived);
    }

    @Test
    public void shouldForwardToChildAtIndex() throws Exception {
        final MockProcessorNode<Object, Object> first = new MockProcessorNode<>(-1);
        final MockProcessorNode<Object, Object> second = new MockProcessorNode<>(-1);
        parentNode.addChild(first);
        parentNode.addChild(second);
        processorRecordContext.forward("key", "value", 1);
        assertEquals(0, first.numReceived);
        assertEquals(1, second.numReceived);
    }

    @Test
    public void shouldForwardToChildWithName() throws Exception {
        final MockProcessorNode<Object, Object> first = new MockProcessorNode<>(-1);
        final MockProcessorNode<Object, Object> second = new MockProcessorNode<>(-1);
        parentNode.addChild(first);
        parentNode.addChild(second);
        processorRecordContext.forward("key", "value", first.name());
        assertEquals(1, first.numReceived);
        assertEquals(0, second.numReceived);
    }

}