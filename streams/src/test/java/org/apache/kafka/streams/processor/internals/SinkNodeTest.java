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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.Test;

import java.io.File;
import java.util.Map;

public class SinkNodeTest {

    @Test(expected = StreamsException.class)
    @SuppressWarnings("unchecked")
    public void invalidInputRecordTimestampTest() {
        final Serializer anySerializer = Serdes.Bytes().serializer();

        final SinkNode sink = new SinkNode<>("name", "output-topic", anySerializer, anySerializer, null);
        sink.init(new MockProcessorContext());

        sink.process(null, null);
    }

    private final class MockProcessorContext implements ProcessorContext, RecordCollector.Supplier {
        private final long invalidTimestamp = -1;

        @Override
        public String applicationId() {
            return null;
        }

        @Override
        public TaskId taskId() {
            return null;
        }

        @Override
        public Serde<?> keySerde() {
            return null;
        }

        @Override
        public Serde<?> valueSerde() {
            return null;
        }

        @Override
        public File stateDir() {
            return null;
        }

        @Override
        public StreamsMetrics metrics() {
            return null;
        }

        @Override
        public void register(StateStore store, boolean loggingEnabled, StateRestoreCallback stateRestoreCallback) {
        }

        @Override
        public StateStore getStateStore(String name) {
            return null;
        }

        @Override
        public void schedule(long interval) {
        }

        @Override
        public <K, V> void forward(K key, V value) {
        }

        @Override
        public <K, V> void forward(K key, V value, int childIndex) {
        }

        @Override
        public <K, V> void forward(K key, V value, String childName) {
        }

        @Override
        public void commit() {
        }

        @Override
        public String topic() {
            return null;
        }

        @Override
        public int partition() {
            return 0;
        }

        @Override
        public long offset() {
            return 0;
        }

        @Override
        public long timestamp() {
            return invalidTimestamp;
        }

        @Override
        public Map<String, Object> appConfigs() {
            return null;
        }

        @Override
        public Map<String, Object> appConfigsWithPrefix(String prefix) {
            return null;
        }

        @Override
        public RecordCollector recordCollector() {
            return null;
        }
    }

}