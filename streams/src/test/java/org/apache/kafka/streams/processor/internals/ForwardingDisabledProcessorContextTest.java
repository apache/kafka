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

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.Record;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertThrows;

@RunWith(EasyMockRunner.class)
public class ForwardingDisabledProcessorContextTest {
    @Mock(MockType.NICE)
    private InternalProcessorContext<String, String> delegate;
    private ForwardingDisabledProcessorContext<String, String> context;

    @Before
    public void setUp() {
        context = new ForwardingDisabledProcessorContext<>(delegate);
    }

    @Test
    public void shouldThrowOnForward() {
        assertThrows(StreamsException.class, () -> context.forward(new Record<>("key", "value", 0L)));
    }

    @Test
    public void shouldThrowOnOldForward() {
        assertThrows(StreamsException.class, () -> context.forward("key", "value"));
    }

    @Test
    public void shouldThrowOnOldForwardWithTo() {
        assertThrows(StreamsException.class, () -> context.forward("key", "value", To.all()));
    }
}