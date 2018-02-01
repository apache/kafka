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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test checks RocksDBStore behaviour with serializer, which
 * serializes null value into non-null byte array.
 */
public class RocksDBStoreCustomNullSerdeTest {
    /*
    private RocksDBStore<String, String> subject;
    private MockProcessorContext context;

    @Before
    public void setUp() throws Exception {
        final Serializer<String> serializer = new StringSerializer() {
            @Override
            public byte[] serialize(final String topic, final String data) {
                if (data == null) {
                    return "null-encoding-that-is-not-just-'null'".getBytes();
                }
                return super.serialize(topic, data);
            }
        };
        final Serde<String> serde = Serdes.serdeFrom(serializer, new StringDeserializer());
        subject = new RocksDBStore<>("test", serde, serde);
        context = new MockProcessorContext(
                TestUtils.tempDirectory(),
                serde,
                serde,
                new NoOpRecordCollector(),
                new ThreadCache("testCache", 0, new MockStreamsMetrics(new Metrics())));
    }

    @After
    public void tearDown() throws Exception {
        subject.close();
    }

    @Test
    public void shouldNotReturnDeletedInIterator() {
        subject.init(context, subject);
        subject.put("a", "1");
        subject.put("b", "2");
        subject.delete("a");
        final KeyValueIterator<String, String> it = subject.all();
        while (it.hasNext()) {
            final KeyValue<String, String> next = it.next();
            if (next.key.equals("a")) {
                Assert.fail("Got deleted key from iterator");
            }
        }
    }
    */
}
