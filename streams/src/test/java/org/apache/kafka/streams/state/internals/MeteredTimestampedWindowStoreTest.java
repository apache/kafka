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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.MockInternalProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class MeteredTimestampedWindowStoreTest {
    private MockInternalProcessorContext context;
    private final WindowStore<Bytes, byte[]> innerStoreMock = EasyMock.createNiceMock(WindowStore.class);
    private final MeteredTimestampedWindowStore<String, String> store = new MeteredTimestampedWindowStore<>(
        innerStoreMock,
        10L, // any size
        "scope",
        new MockTime(),
        Serdes.String(),
        new ValueAndTimestampSerde<>(new SerdeThatDoesntHandleNull())
    );

    {
        EasyMock.expect(innerStoreMock.name()).andReturn("mocked-store").anyTimes();
    }

    @Before
    public void setUp() {
        final Properties properties = StreamsTestUtils.getStreamsConfig();
        properties.put(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, StreamsConfig.METRICS_LATEST);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);
        context = new MockInternalProcessorContext(properties, new Metrics());
    }

    @Test
    public void shouldCloseUnderlyingStore() {
        innerStoreMock.close();
        EasyMock.expectLastCall();
        EasyMock.replay(innerStoreMock);

        store.init(context, store);
        store.close();
        EasyMock.verify(innerStoreMock);
    }

    @Test
    public void shouldNotExceptionIfFetchReturnsNull() {
        EasyMock.expect(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), 0)).andReturn(null);
        EasyMock.replay(innerStoreMock);

        store.init(context, store);
        assertNull(store.fetch("a", 0));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldNotThrowExceptionIfSerdesCorrectlySetFromProcessorContext() {
        EasyMock.expect(innerStoreMock.name()).andStubReturn("mocked-store");
        EasyMock.replay(innerStoreMock);
        final MeteredTimestampedWindowStore<String, Long> store = new MeteredTimestampedWindowStore<>(
            innerStoreMock,
            10L, // any size
            "scope",
            new MockTime(),
            null,
            null
        );
        store.init(context, innerStoreMock);

        try {
            store.put("key", ValueAndTimestamp.make(42L, 60000));
        } catch (final StreamsException exception) {
            if (exception.getCause() instanceof ClassCastException) {
                fail("Serdes are not correctly set from processor context.");
            }
            throw exception;
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldNotThrowExceptionIfSerdesCorrectlySetFromConstructorParameters() {
        EasyMock.expect(innerStoreMock.name()).andStubReturn("mocked-store");
        EasyMock.replay(innerStoreMock);
        final MeteredTimestampedWindowStore<String, Long> store = new MeteredTimestampedWindowStore<>(
            innerStoreMock,
            10L, // any size
            "scope",
            new MockTime(),
            Serdes.String(),
            new ValueAndTimestampSerde<>(Serdes.Long())
        );
        store.init(context, innerStoreMock);

        try {
            store.put("key", ValueAndTimestamp.make(42L, 60000));
        } catch (final StreamsException exception) {
            if (exception.getCause() instanceof ClassCastException) {
                fail("Serdes are not correctly set from constructor parameters.");
            }
            throw exception;
        }
    }
}
