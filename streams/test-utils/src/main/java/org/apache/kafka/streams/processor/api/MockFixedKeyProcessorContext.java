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
package org.apache.kafka.streams.processor.api;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.TaskId;

import java.io.File;
import java.util.Properties;

/**
 * {@link MockFixedKeyProcessorContext} is a mock {@link FixedKeyProcessorContext} for users to test their {@link FixedKeyProcessor},
 * {@link ValueTransformer}, and {@link org.apache.kafka.streams.kstream.ValueTransformerWithKey} implementations, this class is an
 * extension of the {@link MockProcessorContext}.
 * <p>
 * The tests for this class (org.apache.kafka.streams.FixedKeyMockProcessorContextTest) include several behavioral
 * tests that serve as example usage.
 * <p>
 * Note that this class does not take any automated actions (such as firing scheduled punctuators).
 * It simply captures any data it witnesses.
 * If you require more automated tests, we recommend wrapping your {@link FixedKeyProcessor} in a minimal source-processor-sink
 * {@link Topology} and using the {@link TopologyTestDriver}.
 */
public class MockFixedKeyProcessorContext<K, V> extends MockProcessorContext<K, V> implements
        FixedKeyProcessorContext<K, V> {
    public MockFixedKeyProcessorContext() {
        super();
    }

    public MockFixedKeyProcessorContext(final Properties config) {
        super(config);
    }

    public MockFixedKeyProcessorContext(final Properties config, final TaskId taskId, final File dummyFile) {
        super(config, taskId, dummyFile);
    }

    @Override
    public <K1 extends K, V1 extends V> void forward(final FixedKeyRecord<K1, V1> record) {
        super.forward(new Record<>(record.key(), record.value(), record.timestamp(), record.headers()));
    }

    @Override
    public <K1 extends K, V1 extends V> void forward(final FixedKeyRecord<K1, V1> record,
                                                     final String childName) {
        super.forward(new Record<>(record.key(), record.value(), record.timestamp(), record.headers()),
                childName);
    }
}
