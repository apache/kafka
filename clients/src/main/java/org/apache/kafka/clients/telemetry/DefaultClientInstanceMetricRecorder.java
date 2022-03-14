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
package org.apache.kafka.clients.telemetry;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

/**
 * A recorder that exposes {@link Sensor}s used to record the client instance-level metrics.
 */

public class DefaultClientInstanceMetricRecorder extends MetricRecorder implements ClientInstanceMetricRecorder {

    private static final String GROUP_NAME = "client-telemetry";

    private static final int LATENCY_HISTOGRAM_NUM_BIN = 10;
    private static final int LATENCY_HISTOGRAM_MAX_BIN = 2000; // ms

    private final MetricNameTemplate connectionCreations;

    private final MetricName connectionCount;

    private final MetricNameTemplate connectionErrors;

    private final MetricNameTemplate requestRtt;

    private final MetricNameTemplate requestQueueLatency;

    private final MetricNameTemplate requestQueueCount;

    private final MetricNameTemplate requestSuccess;

    private final MetricNameTemplate requestErrors;

    private final MetricName ioWaitTime;

    public DefaultClientInstanceMetricRecorder(Metrics metrics) {
        super(metrics);

        Set<String> brokerIdTags = appendTags(tags, BROKER_ID_LABEL);
        Set<String> reasonTags = appendTags(tags, REASON_LABEL);
        Set<String> brokerIdRequestTypeTags = appendTags(brokerIdTags, REQUEST_TYPE_LABEL);
        Set<String> brokerIdRequestTypeReasonTags = appendTags(brokerIdRequestTypeTags, REASON_LABEL);

        this.connectionCreations = createMetricNameTemplate(CONNECTION_CREATIONS_NAME, GROUP_NAME, CONNECTION_CREATIONS_DESCRIPTION, brokerIdTags);
        this.connectionCount = createMetricName(CONNECTION_COUNT_NAME, GROUP_NAME, CONNECTION_COUNT_DESCRIPTION);
        this.connectionErrors = createMetricNameTemplate(CONNECTION_ERRORS_NAME, GROUP_NAME, CONNECTION_ERRORS_DESCRIPTION, reasonTags);
        this.requestRtt = createMetricNameTemplate(REQUEST_RTT_NAME, GROUP_NAME, REQUEST_RTT_DESCRIPTION, brokerIdRequestTypeTags);
        this.requestQueueLatency = createMetricNameTemplate(REQUEST_QUEUE_LATENCY_NAME, GROUP_NAME, REQUEST_QUEUE_LATENCY_DESCRIPTION, brokerIdTags);
        this.requestQueueCount = createMetricNameTemplate(REQUEST_QUEUE_COUNT_NAME, GROUP_NAME, REQUEST_QUEUE_COUNT_DESCRIPTION, brokerIdTags);
        this.requestSuccess = createMetricNameTemplate(REQUEST_SUCCESS_NAME, GROUP_NAME, REQUEST_SUCCESS_NAME_DESCRIPTION, brokerIdRequestTypeTags);
        this.requestErrors = createMetricNameTemplate(REQUEST_ERRORS_NAME, GROUP_NAME, REQUEST_ERRORS_DESCRIPTION, brokerIdRequestTypeReasonTags);
        this.ioWaitTime = createMetricName(IO_WAIT_TIME_NAME, GROUP_NAME, IO_WAIT_TIME_DESCRIPTION);
    }

    @Override
    public void addConnectionCreations(String brokerId, long amount) {
        Map<String, String> metricsTags = Collections.singletonMap(BROKER_ID_LABEL, brokerId);
        sumSensor(connectionCreations, metricsTags).record(amount);
    }

    @Override
    public void incrementConnectionActive(long amount) {
        gaugeUpdateSensor(connectionCount).record(amount);
    }

    @Override
    public void addConnectionErrors(ConnectionErrorReason reason, long amount) {
        Map<String, String> metricsTags = Collections.singletonMap(REASON_LABEL, reason.toString());
        sumSensor(connectionErrors, metricsTags).record(amount);
    }

    @Override
    public void recordRequestRtt(String brokerId, String requestType, long amount) {
        Map<String, String> metricsTags = new HashMap<>();
        metricsTags.put(BROKER_ID_LABEL, brokerId);
        metricsTags.put(REQUEST_TYPE_LABEL, requestType);
        histogramSensor(requestRtt, metricsTags, LATENCY_HISTOGRAM_NUM_BIN, LATENCY_HISTOGRAM_MAX_BIN).record(amount);
    }

    @Override
    public void recordRequestQueueLatency(String brokerId, long amount) {
        Map<String, String> metricsTags = Collections.singletonMap(BROKER_ID_LABEL, brokerId);
        histogramSensor(requestQueueLatency, metricsTags, LATENCY_HISTOGRAM_NUM_BIN, LATENCY_HISTOGRAM_MAX_BIN).record(amount);
    }

    @Override
    public void incrementRequestQueueCount(String brokerId, long amount) {
        Map<String, String> metricsTags = Collections.singletonMap(BROKER_ID_LABEL, brokerId);
        gaugeUpdateSensor(requestQueueCount, metricsTags).record(amount);
    }

    @Override
    public void addRequestSuccess(String brokerId, String requestType, long amount) {
        Map<String, String> metricsTags = new HashMap<>();
        metricsTags.put(BROKER_ID_LABEL, brokerId);
        metricsTags.put(REQUEST_TYPE_LABEL, requestType);
        sumSensor(requestSuccess, metricsTags).record(amount);
    }

    @Override
    public void addRequestErrors(String brokerId, String requestType, RequestErrorReason reason, long amount) {
        Map<String, String> metricsTags = new HashMap<>();
        metricsTags.put(BROKER_ID_LABEL, brokerId);
        metricsTags.put(REQUEST_TYPE_LABEL, requestType);
        metricsTags.put(REASON_LABEL, reason.toString());
        sumSensor(requestErrors, metricsTags).record(amount);
    }

    @Override
    public void recordIoWaitTime(long amount) {
        histogramSensor(ioWaitTime, LATENCY_HISTOGRAM_NUM_BIN, LATENCY_HISTOGRAM_MAX_BIN).record(amount);
    }
}
