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
package org.apache.kafka.streams.state.internals.metrics;

import org.apache.kafka.common.utils.Time;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RocksDBMetricsRecordingTrigger implements Runnable {

    private final Map<String, RocksDBMetricsRecorder> metricsRecordersToTrigger = new ConcurrentHashMap<>();
    private final Time time;

    public RocksDBMetricsRecordingTrigger(final Time time) {
        this.time = time;
    }

    public void addMetricsRecorder(final RocksDBMetricsRecorder metricsRecorder) {
        final String metricsRecorderName = metricsRecorderName(metricsRecorder);
        if (metricsRecordersToTrigger.containsKey(metricsRecorderName)) {
            throw new IllegalStateException("RocksDB metrics recorder for store \"" + metricsRecorder.storeName() +
                "\" of task " + metricsRecorder.taskId().toString() + " has already been added. "
                + "This is a bug in Kafka Streams.");
        }
        metricsRecordersToTrigger.put(metricsRecorderName, metricsRecorder);
    }

    public void removeMetricsRecorder(final RocksDBMetricsRecorder metricsRecorder) {
        final RocksDBMetricsRecorder removedMetricsRecorder =
            metricsRecordersToTrigger.remove(metricsRecorderName(metricsRecorder));
        if (removedMetricsRecorder == null) {
            throw new IllegalStateException("No RocksDB metrics recorder for store "
                + "\"" + metricsRecorder.storeName() + "\" of task " + metricsRecorder.taskId() + " could be found. "
                + "This is a bug in Kafka Streams.");
        }
    }

    private String metricsRecorderName(final RocksDBMetricsRecorder metricsRecorder) {
        return metricsRecorder.taskId().toString() + "-" + metricsRecorder.storeName();
    }

    @Override
    public void run() {
        final long now = time.milliseconds();
        for (final RocksDBMetricsRecorder metricsRecorder : metricsRecordersToTrigger.values()) {
            metricsRecorder.record(now);
        }
    }
}
