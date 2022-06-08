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
package org.apache.kafka.clients;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;

/**
 * Metrics corresponding to the client instance per
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-714%3A+Client+metrics+and+observability#KIP714:Clientmetricsandobservability-Clientinstance-levelmetrics">KIP-714</a>.
 *
 * <p/>
 *
 * These metrics are relevant for all three client types:
 *
 * <ul>
 *     <li>Admin</li>
 *     <li>Consumer</li>
 *     <li>Producer</li>
 * </ul>
 */
public class ClientInstanceMetricsRegistry extends ClientTelemetryMetricsRegistry {

    public enum ConnectionErrorReason {
        auth, close, disconnect, timeout, TLS
    }

    public enum RequestErrorReason {
        disconnect, error, timeout
    }

    private final static String PREFIX = "org.apache.kafka.client.";

    private final static String CONNECTION_CREATIONS_NAME = PREFIX + "connection.creations";

    private final static String CONNECTION_CREATIONS_DESCRIPTION = "Total number of broker connections made.";

    private final static String CONNECTION_ACTIVE_NAME = PREFIX + "connection.active";

    private final static String CONNECTION_ACTIVE_DESCRIPTION = "Current number of broker connections.";

    private final static String CONNECTION_ERRORS_NAME = PREFIX + "connection.errors";

    private final static String CONNECTION_ERRORS_DESCRIPTION = "Total number of broker connection failures.";

    private final static String REQUEST_QUEUE_COUNT_NAME = PREFIX + "request.queue.count";

    private final static String REQUEST_QUEUE_COUNT_DESCRIPTION = "Number of requests in queue waiting to be sent to broker.";

    private final static String REQUEST_SUCCESS_NAME = PREFIX + "request.success";

    private final static String REQUEST_SUCCESS_NAME_DESCRIPTION = "Number of successful requests to broker, that is where a response is received without no request-level error (but there may be per-sub-resource errors, e.g., errors for certain partitions within an OffsetCommitResponse).";

    private final static String REQUEST_ERRORS_NAME = PREFIX + "request.errors";

    private final static String REQUEST_ERRORS_DESCRIPTION = "Number of failed requests.";

    private final static String BROKER_ID_LABEL = "broker_id";

    private final static String REASON_LABEL = "reason";

    private final static String REQUEST_TYPE_LABEL = "request_type";

    private final static String GROUP_NAME = "client-telemetry";

    private final MetricNameTemplate connectionCreations;

    public final MetricName connectionActive;

    private final MetricNameTemplate connectionErrors;

    private final MetricNameTemplate requestQueueCount;

    private final MetricNameTemplate requestSuccess;

    private final MetricNameTemplate requestErrors;

    public ClientInstanceMetricsRegistry(Metrics metrics) {
        super(metrics);

        Set<String> brokerIdTags = appendTags(tags, BROKER_ID_LABEL);
        Set<String> reasonTags = appendTags(tags, REASON_LABEL);
        Set<String> brokerIdRequestTypeTags = appendTags(brokerIdTags, REQUEST_TYPE_LABEL);
        Set<String> brokerIdRequestTypeReasonTags = appendTags(brokerIdRequestTypeTags, REASON_LABEL);

        this.connectionCreations = createMetricNameTemplate(CONNECTION_CREATIONS_NAME, CONNECTION_CREATIONS_DESCRIPTION, brokerIdTags);
        this.connectionActive = createMetricName(CONNECTION_ACTIVE_NAME, CONNECTION_ACTIVE_DESCRIPTION);
        this.connectionErrors = createMetricNameTemplate(CONNECTION_ERRORS_NAME, CONNECTION_ERRORS_DESCRIPTION, reasonTags);
        this.requestQueueCount = createMetricNameTemplate(REQUEST_QUEUE_COUNT_NAME, REQUEST_QUEUE_COUNT_DESCRIPTION, brokerIdTags);
        this.requestSuccess = createMetricNameTemplate(REQUEST_SUCCESS_NAME, REQUEST_SUCCESS_NAME_DESCRIPTION, brokerIdRequestTypeTags);
        this.requestErrors = createMetricNameTemplate(REQUEST_ERRORS_NAME, REQUEST_ERRORS_DESCRIPTION, brokerIdRequestTypeReasonTags);
    }

    private MetricName createMetricName(String name, String description) {
        return createMetricName(name, GROUP_NAME, description);
    }

    private MetricNameTemplate createMetricNameTemplate(String name, String description, Set<String> tags) {
        return new MetricNameTemplate(name, GROUP_NAME, description, tags);
    }

    public MetricName connectionCreations(String brokerId) {
        Map<String, String> metricsTags = Collections.singletonMap(BROKER_ID_LABEL, brokerId);
        return this.metrics.metricInstance(connectionCreations, metricsTags);
    }

    public MetricName connectionErrors(ConnectionErrorReason connectionErrorReason) {
        Map<String, String> metricsTags = Collections.singletonMap(REASON_LABEL, connectionErrorReason.toString());
        return this.metrics.metricInstance(connectionErrors, metricsTags);
    }

    public MetricName requestQueueCount(String brokerId) {
        Map<String, String> metricsTags = Collections.singletonMap(BROKER_ID_LABEL, brokerId);
        return this.metrics.metricInstance(requestQueueCount, metricsTags);
    }

    public MetricName requestSuccess(String brokerId, String requestType) {
        Map<String, String> metricsTags = new HashMap<>();
        metricsTags.put(BROKER_ID_LABEL, brokerId);
        metricsTags.put(REQUEST_TYPE_LABEL, requestType);
        return this.metrics.metricInstance(requestSuccess, metricsTags);
    }

    public MetricName requestErrors(String brokerId, String requestType, RequestErrorReason reason) {
        Map<String, String> metricsTags = new HashMap<>();
        metricsTags.put(BROKER_ID_LABEL, brokerId);
        metricsTags.put(REQUEST_TYPE_LABEL, requestType);
        metricsTags.put(REASON_LABEL, reason.toString());
        return this.metrics.metricInstance(requestErrors, metricsTags);
    }

}
