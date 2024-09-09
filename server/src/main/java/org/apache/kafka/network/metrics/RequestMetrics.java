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
package org.apache.kafka.network.metrics;

import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class RequestMetrics {

    public static final String CONSUMER_FETCH_METRIC_NAME = ApiKeys.FETCH.name + "Consumer";
    public static final String FOLLOW_FETCH_METRIC_NAME = ApiKeys.FETCH.name + "Follower";
    public static final String VERIFY_PARTITIONS_IN_TXN_METRIC_NAME = ApiKeys.ADD_PARTITIONS_TO_TXN.name + "Verification";
    public static final String REQUESTS_PER_SEC = "RequestsPerSec";
    public static final String DEPRECATED_REQUESTS_PER_SEC = "DeprecatedRequestsPerSec";
    public static final String MESSAGE_CONVERSIONS_TIME_MS = "MessageConversionsTimeMs";
    public static final String TEMPORARY_MEMORY_BYTES = "TemporaryMemoryBytes";

    private static final String REQUEST_QUEUE_TIME_MS = "RequestQueueTimeMs";
    private static final String LOCAL_TIME_MS = "LocalTimeMs";
    private static final String REMOTE_TIME_MS = "RemoteTimeMs";
    private static final String THROTTLE_TIME_MS = "ThrottleTimeMs";
    private static final String RESPONSE_QUEUE_TIME_MS = "ResponseQueueTimeMs";
    private static final String RESPONSE_SEND_TIME_MS = "ResponseSendTimeMs";
    private static final String TOTAL_TIME_MS = "TotalTimeMs";
    private static final String REQUEST_BYTES = "RequestBytes";
    private static final String ERRORS_PER_SEC = "ErrorsPerSec";

    // time a request spent in a request queue
    public final Histogram requestQueueTimeHist;
    // time a request takes to be processed at the local broker
    public final Histogram localTimeHist;
    // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
    public final Histogram remoteTimeHist;
    // time a request is throttled, not part of the request processing time (throttling is done at the client level
    // for clients that support KIP-219 and by muting the channel for the rest)
    public final Histogram throttleTimeHist;
    // time a response spent in a response queue
    public final Histogram responseQueueTimeHist;
    // time to send the response to the requester
    public final Histogram responseSendTimeHist;
    public final Histogram totalTimeHist;
    // request size in bytes
    public final Histogram requestBytesHist;
    // time for message conversions (only relevant to fetch and produce requests)
    public final Optional<Histogram> messageConversionsTimeHist;
    // Temporary memory allocated for processing request (only populated for fetch and produce requests)
    // This shows the memory allocated for compression/conversions excluding the actual request size
    public final Optional<Histogram> tempMemoryBytesHist;

    // For compatibility - this metrics group was previously defined within
    // a Scala class named `kafka.network.RequestMetrics`
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup("kafka.network", "RequestMetrics");
    private final String name;
    private final Map<String, String> tags;
    private final ConcurrentMap<Short, Meter> requestRateInternal = new ConcurrentHashMap<>();
    private final ConcurrentMap<DeprecatedRequestRateKey, Meter> deprecatedRequestRateInternal = new ConcurrentHashMap<>();
    private final Map<Errors, ErrorMeter> errorMeters = new HashMap<>();

    public RequestMetrics(String name) {
        this.name = name;
        tags = Collections.singletonMap("request", name);
        // time a request spent in a request queue
        requestQueueTimeHist = metricsGroup.newHistogram(REQUEST_QUEUE_TIME_MS, true, tags);
        // time a request takes to be processed at the local broker
        localTimeHist = metricsGroup.newHistogram(LOCAL_TIME_MS, true, tags);
        // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
        remoteTimeHist = metricsGroup.newHistogram(REMOTE_TIME_MS, true, tags);
        // time a request is throttled, not part of the request processing time (throttling is done at the client level
        // for clients that support KIP-219 and by muting the channel for the rest)
        throttleTimeHist = metricsGroup.newHistogram(THROTTLE_TIME_MS, true, tags);
        // time a response spent in a response queue
        responseQueueTimeHist = metricsGroup.newHistogram(RESPONSE_QUEUE_TIME_MS, true, tags);
        // time to send the response to the requester
        responseSendTimeHist = metricsGroup.newHistogram(RESPONSE_SEND_TIME_MS, true, tags);
        totalTimeHist = metricsGroup.newHistogram(TOTAL_TIME_MS, true, tags);
        // request size in bytes
        requestBytesHist = metricsGroup.newHistogram(REQUEST_BYTES, true, tags);
        // time for message conversions (only relevant to fetch and produce requests)
        messageConversionsTimeHist = isFetchOrProduce(name)
                ? Optional.of(metricsGroup.newHistogram(MESSAGE_CONVERSIONS_TIME_MS, true, tags))
                : Optional.empty();
        // Temporary memory allocated for processing request (only populated for fetch and produce requests)
        // This shows the memory allocated for compression/conversions excluding the actual request size
        tempMemoryBytesHist = isFetchOrProduce(name)
                ? Optional.of(metricsGroup.newHistogram(TEMPORARY_MEMORY_BYTES, true, tags))
                : Optional.empty();
        for (Errors error : Errors.values()) {
            errorMeters.put(error, new ErrorMeter(name, error));
        }
    }

    public Meter requestRate(short version) {
        return requestRateInternal.computeIfAbsent(version, v -> metricsGroup.newMeter(REQUESTS_PER_SEC, "requests", TimeUnit.SECONDS, tagsWithVersion(v)));
    }

    public Optional<Meter> deprecatedRequestRate(ApiKeys apiKey, short version, ClientInformation clientInformation) {
        if (apiKey.isVersionDeprecated(version)) {
            return Optional.of(deprecatedRequestRateInternal.computeIfAbsent(new DeprecatedRequestRateKey(version, clientInformation),
                k -> metricsGroup.newMeter(DEPRECATED_REQUESTS_PER_SEC, "requests", TimeUnit.SECONDS, tagsWithVersionAndClientInfo(version, clientInformation))));
        } else {
            return Optional.empty();
        }
    }

    public void markErrorMeter(Errors error, int count) {
        errorMeters.get(error).getOrCreateMeter().mark(count);
    }

    private Map<String, String> tagsWithVersion(short version) {
        Map<String, String> nameAndVersionTags = new LinkedHashMap<>((int) Math.ceil((tags.size() + 1) / 0.75)); // take load factor into account
        nameAndVersionTags.putAll(tags);
        nameAndVersionTags.put("version", String.valueOf(version));
        return nameAndVersionTags;
    }

    private Map<String, String> tagsWithVersionAndClientInfo(short version, ClientInformation clientInformation) {
        Map<String, String> extendedTags = new LinkedHashMap<>((int) Math.ceil((tags.size() + 3) / 0.75)); // take load factor into account
        extendedTags.putAll(tags);
        extendedTags.put("version", String.valueOf(version));
        extendedTags.put("clientSoftwareName", clientInformation.softwareName());
        extendedTags.put("clientSoftwareVersion", clientInformation.softwareVersion());
        return extendedTags;
    }

    void removeMetrics() {
        for (short version : requestRateInternal.keySet()) {
            metricsGroup.removeMetric(REQUESTS_PER_SEC, tagsWithVersion(version));
        }
        for (DeprecatedRequestRateKey key : deprecatedRequestRateInternal.keySet()) {
            metricsGroup.removeMetric(DEPRECATED_REQUESTS_PER_SEC, tagsWithVersionAndClientInfo(key.version, key.clientInformation));
        }
        metricsGroup.removeMetric(REQUEST_QUEUE_TIME_MS, tags);
        metricsGroup.removeMetric(LOCAL_TIME_MS, tags);
        metricsGroup.removeMetric(REMOTE_TIME_MS, tags);
        metricsGroup.removeMetric(REQUESTS_PER_SEC, tags);
        metricsGroup.removeMetric(THROTTLE_TIME_MS, tags);
        metricsGroup.removeMetric(RESPONSE_QUEUE_TIME_MS, tags);
        metricsGroup.removeMetric(TOTAL_TIME_MS, tags);
        metricsGroup.removeMetric(RESPONSE_SEND_TIME_MS, tags);
        metricsGroup.removeMetric(REQUEST_BYTES, tags);
        if (isFetchOrProduce(name)) {
            metricsGroup.removeMetric(MESSAGE_CONVERSIONS_TIME_MS, tags);
            metricsGroup.removeMetric(TEMPORARY_MEMORY_BYTES, tags);
        }
        for (ErrorMeter errorMeter : errorMeters.values()) {
            errorMeter.removeMeter();
        }
        errorMeters.clear();
    }

    private boolean isFetchOrProduce(String name) {
        return ApiKeys.FETCH.name.equals(name) || ApiKeys.PRODUCE.name.equals(name);
    }

    private class ErrorMeter {

        private final Map<String, String> tags = new LinkedHashMap<>();
        private volatile Meter meter;

        private ErrorMeter(String name, Errors error) {
            tags.put("request", name);
            tags.put("error", error.name());
        }

        private synchronized Meter getOrCreateMeter() {
            if (meter == null) {
                meter = metricsGroup.newMeter(ERRORS_PER_SEC, "requests", TimeUnit.SECONDS, tags);
            }
            return meter;
        }

        private synchronized void removeMeter() {
            if (meter != null) {
                metricsGroup.removeMetric(ERRORS_PER_SEC, tags);
                meter = null;
            }
        }
    }

    private static class DeprecatedRequestRateKey {

        private final short version;
        private final ClientInformation clientInformation;

        private DeprecatedRequestRateKey(short version, ClientInformation clientInformation) {
            this.version = version;
            this.clientInformation = clientInformation;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DeprecatedRequestRateKey that = (DeprecatedRequestRateKey) o;
            return version == that.version && Objects.equals(clientInformation, that.clientInformation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(version, clientInformation);
        }
    }
}
