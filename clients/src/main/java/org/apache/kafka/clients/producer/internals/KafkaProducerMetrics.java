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

package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;

import java.util.Map;

public class KafkaProducerMetrics implements AutoCloseable {

    public static final String GROUP = "producer-metrics";
    private static final String FLUSH = "flush";
    private static final String TXN_INIT = "txn-init";
    private static final String TXN_BEGIN = "txn-begin";
    private static final String TXN_SEND_OFFSETS = "txn-send-offsets";
    private static final String TXN_COMMIT = "txn-commit";
    private static final String TXN_ABORT = "txn-abort";
    private static final String TOTAL_TIME_SUFFIX = "-time-ns-total";

    private final Map<String, String> tags;
    private final Metrics metrics;
    private final Sensor initTimeSensor;
    private final Sensor beginTxnTimeSensor;
    private final Sensor flushTimeSensor;
    private final Sensor sendOffsetsSensor;
    private final Sensor commitTxnSensor;
    private final Sensor abortTxnSensor;

    public KafkaProducerMetrics(Metrics metrics) {
        this.metrics = metrics;
        tags = this.metrics.config().tags();
        flushTimeSensor = newLatencySensor(
            FLUSH,
            "Total time producer has spent in flush in nanoseconds."
        );
        initTimeSensor = newLatencySensor(
            TXN_INIT,
            "Total time producer has spent in initTransactions in nanoseconds."
        );
        beginTxnTimeSensor = newLatencySensor(
            TXN_BEGIN,
            "Total time producer has spent in beginTransaction in nanoseconds."
        );
        sendOffsetsSensor = newLatencySensor(
            TXN_SEND_OFFSETS,
            "Total time producer has spent in sendOffsetsToTransaction in nanoseconds."
        );
        commitTxnSensor = newLatencySensor(
            TXN_COMMIT,
            "Total time producer has spent in commitTransaction in nanoseconds."
        );
        abortTxnSensor = newLatencySensor(
            TXN_ABORT,
            "Total time producer has spent in abortTransaction in nanoseconds."
        );
    }

    @Override
    public void close() {
        removeMetric(FLUSH);
        removeMetric(TXN_INIT);
        removeMetric(TXN_BEGIN);
        removeMetric(TXN_SEND_OFFSETS);
        removeMetric(TXN_COMMIT);
        removeMetric(TXN_ABORT);
    }

    public void recordFlush(long duration) {
        flushTimeSensor.record(duration);
    }

    public void recordInit(long duration) {
        initTimeSensor.record(duration);
    }

    public void recordBeginTxn(long duration) {
        beginTxnTimeSensor.record(duration);
    }

    public void recordSendOffsets(long duration) {
        sendOffsetsSensor.record(duration);
    }

    public void recordCommitTxn(long duration) {
        commitTxnSensor.record(duration);
    }

    public void recordAbortTxn(long duration) {
        abortTxnSensor.record(duration);
    }

    private Sensor newLatencySensor(String name, String description) {
        Sensor sensor = metrics.sensor(name + TOTAL_TIME_SUFFIX);
        sensor.add(metricName(name, description), new CumulativeSum());
        return sensor;
    }

    private MetricName metricName(final String name, final String description) {
        return metrics.metricName(name + TOTAL_TIME_SUFFIX, GROUP, description, tags);
    }

    private void removeMetric(final String name) {
        metrics.removeSensor(name + TOTAL_TIME_SUFFIX);
    }
}
