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

import static org.apache.kafka.clients.ClientTelemetry.DEFAULT_PUSH_INTERVAL_MS;
import static org.apache.kafka.clients.ClientTelemetry.MAX_TERMINAL_PUSH_WAIT_MS;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.telemetry.emitter.Context;
import org.apache.kafka.common.telemetry.metrics.MetricType;
import org.apache.kafka.clients.ClientInstanceMetricsRegistry.ConnectionErrorReason;
import org.apache.kafka.common.telemetry.metrics.InvalidMetricTypeException;
import org.apache.kafka.common.telemetry.metrics.MetricKeyable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientTelemetryUtils {

    private final static Logger log = LoggerFactory.getLogger(ClientTelemetryUtils.class);

    private final static Map<String, String> PRODUCER_CONFIG_MAPPING = new HashMap<>();

    private final static Map<String, String> CONSUMER_CONFIG_MAPPING = new HashMap<>();

    static {
        PRODUCER_CONFIG_MAPPING.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, Context.TRANSACTIONAL_ID);

        CONSUMER_CONFIG_MAPPING.put(ConsumerConfig.CLIENT_RACK_CONFIG, Context.CLIENT_RACK);
        CONSUMER_CONFIG_MAPPING.put(ConsumerConfig.GROUP_ID_CONFIG, Context.GROUP_ID);
        CONSUMER_CONFIG_MAPPING.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, Context.GROUP_INSTANCE_ID);
    }

    public final static Predicate<? super MetricKeyable> SELECTOR_NO_METRICS = k -> false;

    public final static Predicate<? super MetricKeyable> SELECTOR_ALL_METRICS = k -> true;

    public static Optional<ClientTelemetry> create(ConsumerConfig config, LogContext logContext, Time time, String clientId) {
        return create(config, logContext, time, clientId, CONSUMER_CONFIG_MAPPING);
    }

    public static Optional<ClientTelemetry> create(ProducerConfig config, LogContext logContext, Time time, String clientId) {
        return create(config, logContext, time, clientId, PRODUCER_CONFIG_MAPPING);
    }

    public static Optional<ClientTelemetry> create(AdminClientConfig config, LogContext logContext, Time time, String clientId) {
        return create(config, logContext, time, clientId, null);
    }

    public static Optional<ClientTelemetry> create(boolean enableMetricsPush, LogContext logContext, Time time, String clientId) {
        if (enableMetricsPush)
            return Optional.of(new ClientTelemetry(logContext, time, clientId));
        else
            return Optional.empty();
    }

    private static Optional<ClientTelemetry> create(AbstractConfig config,
        LogContext logContext,
        Time time,
        String clientId,
        Map<String, String> configToContextNameMapping) {
        if (config == null) {
            log.warn("Not able to create client telemetry as config was null");
            return Optional.empty();
        }

        Boolean enableMetricsPush = config.getBoolean(CommonClientConfigs.ENABLE_METRICS_PUSH_CONFIG);

        if (enableMetricsPush != null && !enableMetricsPush) {
            log.debug("Not creating client telemetry as {} was disabled", CommonClientConfigs.ENABLE_METRICS_PUSH_CONFIG);
            return Optional.empty();
        }

        ClientTelemetry clientTelemetry = new ClientTelemetry(logContext, time, clientId);

        if (configToContextNameMapping != null) {
            Context context = clientTelemetry.context();

            for (Map.Entry<String, String> e : configToContextNameMapping.entrySet()) {
                String configName = e.getKey();
                String contextName = e.getValue();

                if (config.getString(configName) != null) {
                    String contextValue = config.getString(configName);
                    log.debug("Adding key/value pair to client telemetry context: {}={}",
                        contextName, contextValue);
                    context.put(contextName, contextValue);
                }
            }
        }

        return Optional.of(clientTelemetry);
    }

    public static Optional<String> clientInstanceId(Optional<ClientTelemetry> clientTelemetry, Duration timeout) {
        return clientTelemetry.isPresent() ? clientTelemetry.get().clientInstanceId(timeout) : Optional.empty();
    }

    public static void initiateTermination(Optional<ClientTelemetry> clientTelemetry, long timeoutMs) {
        // The clientTelemetry can be null if it is called from a close() that was invoked as
        // part of a failed constructor.
        if (clientTelemetry == null)
            return;

        // This starts the client telemetry termination process which will attempt to send a
        // terminal telemetry push, if possible.
        //
        // This is a separate step from actually closing the instance, which we do further down.
        Duration d = Duration.ofMillis(Math.min(MAX_TERMINAL_PUSH_WAIT_MS, timeoutMs));
        clientTelemetry.ifPresent(ct -> ct.initiateClose(d));
    }

    public static void closeQuietly(Optional<ClientTelemetry> clientTelemetry, String name, AtomicReference<Throwable> firstException) {
        // The clientTelemetry can be null if it is called from a close() that was invoked as
        // part of a failed constructor.
        if (clientTelemetry == null)
            return;

        clientTelemetry.ifPresent(ct -> Utils.closeQuietly(ct, name, firstException));
    }

    public static void closeQuietly(Optional<ClientTelemetry> clientTelemetry, String name) {
        // The clientTelemetry can be null if it is called from a close() that was invoked as
        // part of a failed constructor.
        if (clientTelemetry == null)
            return;

        clientTelemetry.ifPresent(ct -> Utils.closeQuietly(ct, name));
    }

    /**
     * Examine the response data and handle different error code accordingly:
     *
     * <ul>
     *     <li>Authorization Failed: Retry 30min later</li>
     *     <li>Invalid Record: Retry 5min later</li>
     *     <li>UnknownSubscription or Unsupported Compression: Retry</li>
     * </ul>
     *
     * @param errorCode response body error code
     */
    public static Optional<Long> errorPushIntervalMs(short errorCode) {
        if (errorCode == Errors.NONE.code())
            return Optional.empty();

        long pushIntervalMs = -1;
        String reason = null;

        // We might want to wait and retry or retry after some failures are received
        if (isAuthorizationFailedError(errorCode)) {
            pushIntervalMs = 30 * 60 * 1000;
            reason = "The client is not authorized to send metrics";
        } else if (errorCode == Errors.INVALID_REQUEST.code()) {
            pushIntervalMs = 30 * 60 * 1000;
            reason = "The broker response indicates the client sent an invalid request";
        } else if (errorCode == Errors.INVALID_RECORD.code()) {
            pushIntervalMs = 5 * 60 * 1000;
            reason = "The broker failed to decode or validate the client’s encoded metrics";
        } else if (errorCode == Errors.CLIENT_METRICS_PLUGIN_NOT_FOUND.code()) {
            pushIntervalMs = DEFAULT_PUSH_INTERVAL_MS;
            reason = "The broker does not have any client metrics plugin configured";
        } else if (errorCode == Errors.UNKNOWN_CLIENT_METRICS_SUBSCRIPTION_ID.code() ||
            errorCode == Errors.UNSUPPORTED_COMPRESSION_TYPE.code()) {
            pushIntervalMs = 0;
            reason = Errors.forCode(errorCode).message();
        }

        if (pushIntervalMs >= 0) {
            log.warn("Error code: {}, reason: {}. Retry automatically in {} ms.", errorCode, reason, pushIntervalMs);
            return Optional.of(pushIntervalMs);
        } else {
            return Optional.empty();
        }
    }

    public static boolean isAuthorizationFailedError(short errorCode) {
        return errorCode == Errors.CLUSTER_AUTHORIZATION_FAILED.code() ||
            errorCode == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code() ||
            errorCode == Errors.GROUP_AUTHORIZATION_FAILED.code() ||
            errorCode == Errors.TOPIC_AUTHORIZATION_FAILED.code() ||
            errorCode == Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED.code();
    }

    public static Predicate<? super MetricKeyable> validateMetricNames(List<String> requestedMetrics) {
        if (requestedMetrics == null || requestedMetrics.isEmpty()) {
            log.debug("Telemetry subscription has specified no metric names; telemetry will record no metrics");
            return SELECTOR_NO_METRICS;
        } else if (requestedMetrics.size() == 1 && requestedMetrics.get(0).isEmpty()) {
            log.debug("Telemetry subscription has specified a single empty metric name; using all metrics");
            return SELECTOR_ALL_METRICS;
        } else {
            log.debug("Telemetry subscription has specified to include only metrics that are prefixed with the following strings: {}", requestedMetrics);
            return k -> requestedMetrics.stream().anyMatch(f -> k.key().name().startsWith(f));
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

    public static int validatePushIntervalMs(final int pushIntervalMs) {
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
        } else if (measurable instanceof CumulativeSum) {
            return MetricType.sum;
        } else {
            throw new InvalidMetricTypeException("Could not determine metric type from measurable type " + measurable + " of metric " + kafkaMetric);
        }
    }

    public static ByteBuffer serialize(byte[] raw, CompressionType compressionType) {

        try {
            try (ByteBufferOutputStream compressedOut = new ByteBufferOutputStream(1024)) {
                try (OutputStream out = compressionType.wrapForOutput(compressedOut, RecordBatch.CURRENT_MAGIC_VALUE)) {
                    out.write(raw);
                    out.flush();
                }

                return (ByteBuffer) compressedOut.buffer().flip();
            }
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    public static String convertToReason(Throwable error) {
        return String.valueOf(error);
    }

    public static Optional<ConnectionErrorReason> convertToConnectionErrorReason(Errors errors) {
        switch (errors) {
            case NETWORK_EXCEPTION:
                return Optional.of(ConnectionErrorReason.disconnect);

            case CLUSTER_AUTHORIZATION_FAILED:
            case DELEGATION_TOKEN_AUTHORIZATION_FAILED:
            case DELEGATION_TOKEN_AUTH_DISABLED:
            case GROUP_AUTHORIZATION_FAILED:
            case SASL_AUTHENTICATION_FAILED:
            case TOPIC_AUTHORIZATION_FAILED:
            case TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
                return Optional.of(ConnectionErrorReason.auth);

            case REQUEST_TIMED_OUT:
                return Optional.of(ConnectionErrorReason.timeout);

            default:
                return Optional.empty();
        }
    }

    public static int calculateQueueBytes(ApiVersions apiVersions,
        long timestamp,
        byte[] key,
        byte[] value,
        Header[] headers) {
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

}
