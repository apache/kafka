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

import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;

public class KafkaProducerMetrics implements AutoCloseable {

    public static final String GROUP = "producer-metrics";
    private static final String FLUSH = "flush";
    private static final String TXN_INIT = "txn-init";
    private static final String TXN_BEGIN = "txn-begin";
    private static final String TXN_SEND_OFFSETS = "txn-send-offsets";
    private static final String TXN_COMMIT = "txn-commit";
    private static final String TXN_ABORT = "txn-abort";
    private static final String TOTAL_TIME_SUFFIX = "-time-total";

    final Map<String, String> tags;
    final Metrics metrics;
    final Sensor initTimeSensor;
    final Sensor beginTxnTimeSensor;
    final Sensor flushTimeSensor;
    final Sensor sendOffsetsSensor;
    final Sensor commitTxnSensor;
    final Sensor abortTxnSensor;

    public KafkaProducerMetrics(Metrics metrics) {
        this.metrics = metrics;
        this.tags = this.metrics.config().tags();
        this.flushTimeSensor = newLatencySensor(FLUSH);
        this.initTimeSensor = newLatencySensor(TXN_INIT);
        this.beginTxnTimeSensor = newLatencySensor(TXN_BEGIN);
        this.sendOffsetsSensor = newLatencySensor(TXN_SEND_OFFSETS);
        this.commitTxnSensor = newLatencySensor(TXN_COMMIT);
        this.abortTxnSensor = newLatencySensor(TXN_ABORT);
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

    private Sensor newLatencySensor(String name) {
        Sensor sensor = metrics.sensor(name + TOTAL_TIME_SUFFIX);
        sensor.add(metricName(name), new CumulativeSum());
        return sensor;
    }

    private MetricName metricName(final String name) {
        return metrics.metricName(name + TOTAL_TIME_SUFFIX, GROUP, tags);
    }

    private void removeMetric(final String name) {
        metrics.removeSensor(name + TOTAL_TIME_SUFFIX);
        metrics.removeMetric(metricName(name));
    }
}
