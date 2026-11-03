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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

class WorkerShareSinkTaskContext implements SinkTaskContext {

    private final Supplier<Map<String, String>> configs;
    private final PluginMetrics pluginMetrics;
    // Retry and commit hints are confined to the serial task lane.
    private long timeoutMs = -1L;
    private boolean commitRequested;

    WorkerShareSinkTaskContext(Supplier<Map<String, String>> configs, PluginMetrics pluginMetrics) {
        this.configs = Objects.requireNonNull(configs);
        this.pluginMetrics = Objects.requireNonNull(pluginMetrics);
    }

    @Override
    public Map<String, String> configs() {
        return Collections.unmodifiableMap(new HashMap<>(configs.get()));
    }

    @Override
    public void offset(Map<TopicPartition, Long> offsets) {
        throw new UnsupportedOperationException("Share sink tasks cannot reset offsets");
    }

    @Override
    public void offset(TopicPartition tp, long offset) {
        throw new UnsupportedOperationException("Share sink tasks cannot reset offsets");
    }

    @Override
    public void timeout(long timeoutMs) {
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("Retry timeout must be non-negative");
        }
        this.timeoutMs = timeoutMs;
    }

    @Override
    public Set<TopicPartition> assignment() {
        throw new UnsupportedOperationException("Share sink tasks do not have exclusive partition assignments");
    }

    @Override
    public void pause(TopicPartition... partitions) {
        throw new UnsupportedOperationException("Share sink tasks cannot pause partitions");
    }

    @Override
    public void resume(TopicPartition... partitions) {
        throw new UnsupportedOperationException("Share sink tasks cannot resume partitions");
    }

    @Override
    public void requestCommit() {
        commitRequested = true;
    }

    Hints drainHints() {
        Hints result = new Hints(timeoutMs, commitRequested);
        timeoutMs = -1L;
        commitRequested = false;
        return result;
    }

    @Override
    public ErrantRecordReporter errantRecordReporter() {
        throw new UnsupportedOperationException("Share sink tasks do not support errant record reporting");
    }

    @Override
    public PluginMetrics pluginMetrics() {
        return pluginMetrics;
    }

    record Hints(long timeoutMs, boolean commitRequested) {
    }
}
