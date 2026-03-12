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
package org.apache.kafka.clients.consumer.internals.metrics;

import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.WindowedCount;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Utility class that serves as a common abstraction point for consumers to create and register their
 * metrics, and to ensure they're removed on {@link #close()}.
 */
public abstract class AbstractConsumerMetricsManager implements AutoCloseable {

    protected final RecordingMetrics recordingMetrics;

    protected AbstractConsumerMetricsManager(RecordingMetrics recordingMetrics) {
        this.recordingMetrics = Objects.requireNonNull(recordingMetrics);
    }

    protected final Meter createMeter(String groupName, String baseName, String descriptiveName) {
        return new Meter(new WindowedCount(),
            recordingMetrics.metricName(baseName + "-rate", groupName,
                String.format("The number of %s per second", descriptiveName)),
            recordingMetrics.metricName(baseName + "-total", groupName,
                String.format("The total number of %s", descriptiveName)));
    }

    protected SensorBuilder sensorBuilder(String name) {
        return new SensorBuilder(recordingMetrics, name);
    }

    protected SensorBuilder sensorBuilder(String name, Supplier<Map<String, String>> tagsSupplier) {
        return new SensorBuilder(recordingMetrics, name, tagsSupplier);
    }

    @Override
    public final void close() {
        recordingMetrics.close();
    }
}