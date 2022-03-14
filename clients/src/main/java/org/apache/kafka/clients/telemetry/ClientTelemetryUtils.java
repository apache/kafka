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

import static org.apache.kafka.clients.telemetry.ClientTelemetry.DEFAULT_PUSH_INTERVAL_MS;
import static org.apache.kafka.common.Uuid.ZERO_UUID;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.telemetry.ClientInstanceMetricRecorder.ConnectionErrorReason;
import org.apache.kafka.clients.telemetry.MetricSelector.FilteredMetricSelector;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Histogram;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionRequest;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientTelemetryUtils {

    private static final Logger log = LoggerFactory.getLogger(ClientTelemetryUtils.class);

    @SuppressWarnings("fallthrough")
    public static long timeToNextUpdate(TelemetryState state, TelemetrySubscription subscription, long requestTimeoutMs, Time time) {
        final long t;

        switch (state) {
            case subscription_in_progress:
            case push_in_progress:
            case terminating_push_in_progress:
                // We have network requests in progress, so wait the amount of the requestTimeout
                // as provided.
                t = requestTimeoutMs;
                break;

            case terminating_push_needed:
                t = 0;
                break;

            case subscription_needed:
                if (subscription == null)
                    t = 0;
                else
                    t = timeToNextUpdate(subscription, time);

                break;

            case push_needed:
                if (subscription != null) {
                    t = timeToNextUpdate(subscription, time);
                    break;
                } else {
                    log.warn("Telemetry subscription was null when determining time for next update in state {}", state);
                    // Fall through...
                }

            default:
                // Should never get to here
                t = Long.MAX_VALUE;
        }

        return t;
    }

    public static long timeToNextUpdate(TelemetrySubscription subscription, Time time) {
        long fetchMs = subscription.fetchMs();
        long pushIntervalMs = subscription.pushIntervalMs();
        long timeRemainingBeforePush = fetchMs + pushIntervalMs - time.milliseconds();

        final long t;

        if (timeRemainingBeforePush < 0)
            t = 0;
        else
            t = timeRemainingBeforePush;

        return t;
    }

    public static MetricSelector validateMetricNames(List<String> requestedMetrics) {
        if (requestedMetrics == null || requestedMetrics.isEmpty()) {
            log.trace("Telemetry subscription has specified no metric names; telemetry will record no metrics");
            return MetricSelector.NONE;
        } else if (requestedMetrics.size() == 1 && requestedMetrics.get(0).isEmpty()) {
            log.trace("Telemetry subscription has specified a single empty metric name; using all metrics");
            return MetricSelector.ALL;
        } else {
            log.trace("Telemetry subscription has specified to include only metrics that are prefixed with the following strings: {}", requestedMetrics);
            return new FilteredMetricSelector(requestedMetrics);
        }
    }

    public static List<CompressionType> validateAcceptedCompressionTypes(List<Byte> acceptedCompressionTypes) {
        List<CompressionType> list = new ArrayList<>();

        if (acceptedCompressionTypes != null && !acceptedCompressionTypes.isEmpty()) {
            for (Byte b : acceptedCompressionTypes) {
                int compressionId = b.intValue();

                try {
                    CompressionType compressionType = CompressionType.forId(compressionId);
                    list.add(compressionType);
                } catch (IllegalArgumentException e) {
                    log.warn("Accepted compression type with ID {} provided by broker is not a known compression type; ignoring", compressionId, e);
                }
            }
        }

        return list;
    }

    public static Uuid validateClientInstanceId(Uuid clientInstanceId) {
        if (clientInstanceId == null)
            throw new IllegalArgumentException("clientInstanceId must be non-null");

        return clientInstanceId;
    }

    public static int validatePushIntervalMs(int pushIntervalMs) {
        if (pushIntervalMs <= 0) {
            log.warn("Telemetry subscription push interval value from broker was invalid ({}), substituting a value of {}", pushIntervalMs, DEFAULT_PUSH_INTERVAL_MS);
            return DEFAULT_PUSH_INTERVAL_MS;
        }

        log.debug("Telemetry subscription push interval value from broker was {}", pushIntervalMs);
        return pushIntervalMs;
    }

    public static CompressionType preferredCompressionType(List<CompressionType> acceptedCompressionTypes) {
        if (acceptedCompressionTypes != null && !acceptedCompressionTypes.isEmpty()) {
            // Broker is providing the compression types in order of preference. Grab the
            // first one.
            return acceptedCompressionTypes.get(0);
        } else {
            return CompressionType.NONE;
        }
    }

    public static MetricType metricType(KafkaMetric kafkaMetric) {
        Measurable measurable = kafkaMetric.measurable();

        if (measurable instanceof Gauge) {
            return MetricType.gauge;
        } else if (measurable instanceof Histogram) {
            return MetricType.histogram;
        } else if (measurable instanceof CumulativeSum) {
            return MetricType.sum;
        } else {
            throw new InvalidMetricTypeException("Could not determine metric type from measurable type " + measurable + " of metric " + kafkaMetric);
        }
    }

    public static List<TelemetryMetric> currentTelemetryMetrics(Collection<KafkaMetric> metrics,
        DeltaValueStore deltaValueStore,
        boolean deltaTemporality,
        MetricSelector metricSelector) {
        return metrics.stream().map(kafkaMetric -> {
            MetricName metricName = kafkaMetric.metricName();
            Object metricValue = kafkaMetric.metricValue();

            // TODO: TELEMETRY_TODO: not sure if the metric value is always stored as a double,
            //                       but empirically it seems to be. Not sure if there is a better
            //                       way to handle this.
            double doubleValue = Double.parseDouble(metricValue.toString());
            long value = Double.valueOf(doubleValue).longValue();
            Measurable measurable = kafkaMetric.measurable();

            if (measurable instanceof CumulativeSum && deltaTemporality) {
                Long previousValue = deltaValueStore.getAndSet(kafkaMetric.metricName(), value);
                value = previousValue != null ? value - previousValue : value;
            }

            MetricType metricType = metricType(kafkaMetric);

            return new TelemetryMetric(metricName, metricType, value);
        }).filter(metricSelector).collect(Collectors.toList());
    }

    public static ByteBuffer serialize(Collection<TelemetryMetric> telemetryMetrics,
        CompressionType compressionType,
        TelemetrySerializer telemetrySerializer) {

        try {
            try (ByteBufferOutputStream compressedOut = new ByteBufferOutputStream(1024)) {
                try (OutputStream out = compressionType.wrapForOutput(compressedOut, RecordBatch.CURRENT_MAGIC_VALUE)) {
                    telemetrySerializer.serialize(telemetryMetrics, out);
                }

                return (ByteBuffer) compressedOut.buffer().flip();
            }
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    public static ClientTelemetry create(AbstractConfig config, Time time, String clientId) {
        if (config == null)
            throw new IllegalArgumentException("config for ClientTelemetry cannot be null");

        boolean enableMetricsPush = config.getBoolean(CommonClientConfigs.ENABLE_METRICS_PUSH_CONFIG);
        return create(enableMetricsPush, time, clientId);
    }

    public static ClientTelemetry create(boolean enableMetricsPush, Time time, String clientId) {
        if (enableMetricsPush)
            return new DefaultClientTelemetry(time, clientId);
        else
            return new NoopClientTelemetry();
    }

    public static String convertToReason(Throwable error) {
        // TODO: TELEMETRY_TODO: properly convert the error to a "reason"
        return String.valueOf(error);
    }

    public static ConnectionErrorReason convertToConnectionErrorReason(Errors errors) {
        // TODO: TELEMETRY_TODO: there's no way this mapping is correct...
        switch (errors) {
            case NETWORK_EXCEPTION:
                return ConnectionErrorReason.disconnect;

            case CLUSTER_AUTHORIZATION_FAILED:
            case DELEGATION_TOKEN_AUTHORIZATION_FAILED:
            case DELEGATION_TOKEN_AUTH_DISABLED:
            case GROUP_AUTHORIZATION_FAILED:
            case SASL_AUTHENTICATION_FAILED:
            case TOPIC_AUTHORIZATION_FAILED:
            case TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
                return ConnectionErrorReason.auth;

            case REQUEST_TIMED_OUT:
                return ConnectionErrorReason.timeout;

            default:
                // TODO: TELEMETRY_TODO: I think we might need an "unknown" case in the KIP.
                //       change this VVVVVVVVVVVVVVV
                return ConnectionErrorReason.timeout;
        }
    }

    public static int calculateQueueBytes(ApiVersions apiVersions,
        long timestamp,
        byte[] key,
        byte[] value,
        Header[] headers) {
        // TODO: TELEMETRY_TODO: need to know the proper place to call this
        // TODO: TELEMETRY_TODO: need to know the proper means/place to determine the size
        int offsetDelta = -1;
        byte magic = apiVersions.maxUsableProduceMagic();

        if (magic > RecordBatch.MAGIC_VALUE_V1) {
            return DefaultRecord.sizeInBytes(offsetDelta,
                timestamp,
                key != null ? key.length : 0,
                value != null ? value.length : 0,
                headers);
        } else {
            return LegacyRecord.recordSize(magic,
                key != null ? key.length : 0,
                value != null ? value.length : 0);
        }
    }

    public static void incrementProducerQueueMetrics(ClientTelemetry clientTelemetry,
        ApiVersions apiVersions,
        short acks,
        TopicPartition tp,
        long timestamp,
        byte[] key,
        byte[] value,
        Header[] headers) {
        // TODO: TELEMETRY_TODO: need to know the proper place to call this
        // TODO: TELEMETRY_TODO: need to know the proper means/place to determine the size
        int size = calculateQueueBytes(apiVersions, timestamp, key, value, headers);

        ProducerMetricRecorder producerMetricRecorder = clientTelemetry.producerMetricRecorder();
        ProducerTopicMetricRecorder producerTopicMetricRecorder = clientTelemetry.producerTopicMetricRecorder();

        producerMetricRecorder.incrementRecordQueueBytes(size);
        producerMetricRecorder.incrementRecordQueueCount(1);

        producerTopicMetricRecorder.incrementRecordQueueBytes(tp, acks, size);
        producerTopicMetricRecorder.incrementRecordQueueCount(tp, acks, 1);
    }

    public static void decrementProducerQueueMetrics(ClientTelemetry clientTelemetry,
        short acks,
        TopicPartition tp,
        int size) {
        // TODO: TELEMETRY_TODO: we need an accurate record count passed in. I don't yet know
        //       how to get it from the RecordBatch or MemoryRecord or ???
        // TODO: TELEMETRY_TODO: need to know the proper place to call this
        // TODO: TELEMETRY_TODO: need to know the proper means/place to determine the size
        int recordCount = 0;

        ProducerMetricRecorder producerMetricRecorder = clientTelemetry.producerMetricRecorder();
        ProducerTopicMetricRecorder producerTopicMetricRecorder = clientTelemetry.producerTopicMetricRecorder();

        producerMetricRecorder.decrementRecordQueueBytes(size);
        producerMetricRecorder.decrementRecordQueueCount(recordCount);
        producerTopicMetricRecorder.decrementRecordQueueBytes(tp, acks, size);
        producerTopicMetricRecorder.decrementRecordQueueCount(tp, acks, recordCount);
    }

    public static String formatAcks(short acks) {
        // TODO: TELEMETRY_TODO: this mapping needs to be verified
        switch (acks) {
            case 0:
                return "none";

            case 1:
                return "leader";

            default:
                return "all";
        }
    }

    public static GetTelemetrySubscriptionRequest.Builder createGetTelemetrySubscriptionRequest(TelemetrySubscription subscription) {
        Uuid clientInstanceId;

        // If we've previously retrieved a subscription, it will contain the client instance ID
        // that the broker assigned. Otherwise, per KIP-714, we send a special "null" UUID to
        // signal to the broker that we need to have a client instance ID assigned.
        if (subscription != null)
            clientInstanceId = subscription.clientInstanceId();
        else
            clientInstanceId = ZERO_UUID;

        return new GetTelemetrySubscriptionRequest.Builder(clientInstanceId);
    }

    public static PushTelemetryRequest.Builder createPushTelemetryRequest(boolean terminating,
        TelemetrySubscription subscription,
        TelemetrySerializer telemetrySerializer,
        Collection<TelemetryMetric> telemetryMetrics) {
        CompressionType compressionType = preferredCompressionType(subscription.acceptedCompressionTypes());

        ByteBuffer buf = serialize(telemetryMetrics, compressionType, telemetrySerializer);
        Bytes metricsData =  Bytes.wrap(Utils.readBytes(buf));

        return new PushTelemetryRequest.Builder(subscription.clientInstanceId(),
            subscription.subscriptionId(),
            terminating,
            compressionType,
            metricsData);
    }

}
