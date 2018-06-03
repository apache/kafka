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
package org.apache.kafka.test;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.AbstractProcessorContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class NoOpProcessorContext extends AbstractProcessorContext {
    public boolean initialized;
    public Map forwardedValues = new HashMap();

    public NoOpProcessorContext() {
        super(new TaskId(1, 1), streamsConfig(), new MockStreamsMetrics(new Metrics()), null, null);
    }

    static StreamsConfig streamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "boot");
        return new StreamsConfig(props);
    }

    @Override
    public StateStore getStateStore(final String name) {
        return null;
    }

    @Override public Cancellable schedule(long interval, PunctuationType type, Punctuator callback) {
        return null;
    }

    @Override
    public <K, V> void forward(final K key, final V value) {
        forwardedValues.put(key, value);
    }

    @Override
    public <K, V> void forward(final K key, final V value, final To to) {
        forwardedValues.put(key, value);
    }

    @Override
    public <K, V> void forward(final K key, final V value, final int childIndex) {
        forward(key, value);
    }

    @Override
    public <K, V> void forward(final K key, final V value, final String childName) {
        forward(key, value);
    }

    @Override
    public void commit() {
    }

    @Override
    public void initialized() {
        initialized = true;
    }

    @Override
    public void register(final StateStore store,
                         final StateRestoreCallback stateRestoreCallback) {
        // no-op
    }
}
