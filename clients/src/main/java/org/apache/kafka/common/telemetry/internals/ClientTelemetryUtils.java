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
package org.apache.kafka.common.telemetry.internals;

import io.opentelemetry.proto.metrics.v1.MetricsData;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

public class ClientTelemetryUtils {

    private final static Logger log = LoggerFactory.getLogger(ClientTelemetryUtils.class);

    public final static Predicate<? super MetricKeyable> SELECTOR_NO_METRICS = k -> false;

    public final static Predicate<? super MetricKeyable> SELECTOR_ALL_METRICS = k -> true;

    /**
     * Examine the response data and handle different error code accordingly:
     *
     * <ul>
     *     <li>Invalid Request: Disable Telemetry</li>
     *     <li>Invalid Record: Disable Telemetry</li>
     *     <li>Unsupported Version: Disable Telemetry</li>
     *     <li>UnknownSubscription or Unsupported Compression: Retry immediately</li>
     *     <li>TelemetryTooLarge or ThrottlingQuotaExceeded: Retry as per next interval</li>
     * </ul>
     *
     * @param errorCode response body error code
     * @param intervalMs current push interval in milliseconds
     *
     * @return Optional of push interval in milliseconds
     */
    public static Optional<Integer> maybeFetchErrorIntervalMs(short errorCode, int intervalMs) {
        if (errorCode == Errors.NONE.code())
            return Optional.empty();

        int pushIntervalMs;
        String reason;

        Errors error = Errors.forCode(errorCode);
        switch (error) {
            case INVALID_REQUEST:
            case INVALID_RECORD:
            case UNSUPPORTED_VERSION:
                pushIntervalMs = Integer.MAX_VALUE;
                reason = "The broker response indicates the client sent an request that cannot be resolved"
                    + " by re-trying, hence disable telemetry";
                break;
            case UNKNOWN_SUBSCRIPTION_ID:
            case UNSUPPORTED_COMPRESSION_TYPE:
                pushIntervalMs = 0;
                reason = error.message();
                break;
            case TELEMETRY_TOO_LARGE:
            case THROTTLING_QUOTA_EXCEEDED:
                reason = error.message();
                pushIntervalMs = (intervalMs != -1) ? intervalMs : ClientTelemetryReporter.DEFAULT_PUSH_INTERVAL_MS;
                break;
            default:
                reason = "Unwrapped error code";
                log.error("Error code: {}. Unmapped error for telemetry, disable telemetry.", errorCode);
                pushIntervalMs = Integer.MAX_VALUE;
        }

        log.debug("Error code: {}, reason: {}. Push interval update to {} ms.", errorCode, reason, pushIntervalMs);
        return Optional.of(pushIntervalMs);
    }

    public static Predicate<? super MetricKeyable> getSelectorFromRequestedMetrics(List<String> requestedMetrics) {
        if (requestedMetrics == null || requestedMetrics.isEmpty()) {
            log.debug("Telemetry subscription has specified no metric names; telemetry will record no metrics");
            return SELECTOR_NO_METRICS;
        } else if (requestedMetrics.size() == 1 && requestedMetrics.get(0) != null && requestedMetrics.get(0).equals("*")) {
            log.debug("Telemetry subscription has specified a single '*' metric name; using all metrics");
            return SELECTOR_ALL_METRICS;
        } else {
            log.debug("Telemetry subscription has specified to include only metrics that are prefixed with the following strings: {}", requestedMetrics);
            return k -> requestedMetrics.stream().anyMatch(f -> k.key().name().startsWith(f));
        }
    }

    public static List<CompressionType> getCompressionTypesFromAcceptedList(List<Byte> acceptedCompressionTypes) {
        if (acceptedCompressionTypes == null || acceptedCompressionTypes.isEmpty()) {
            return Collections.emptyList();
        }

        List<CompressionType> result = new ArrayList<>();
        for (Byte compressionByte : acceptedCompressionTypes) {
            int compressionId = compressionByte.intValue();
            try {
                CompressionType compressionType = CompressionType.forId(compressionId);
                result.add(compressionType);
            } catch (IllegalArgumentException e) {
                log.warn("Accepted compressionByte type with ID {} is not a known compressionByte type; ignoring", compressionId, e);
            }
        }
        return result;
    }

    public static Uuid validateClientInstanceId(Uuid clientInstanceId) {
        if (clientInstanceId == null || clientInstanceId == Uuid.ZERO_UUID) {
            throw new IllegalArgumentException("clientInstanceId is not valid");
        }

        return clientInstanceId;
    }

    public static int validateIntervalMs(int intervalMs) {
        if (intervalMs <= 0) {
            log.warn("Telemetry subscription push interval value from broker was invalid ({}),"
                + " substituting with default value of {}", intervalMs, ClientTelemetryReporter.DEFAULT_PUSH_INTERVAL_MS);
            return ClientTelemetryReporter.DEFAULT_PUSH_INTERVAL_MS;
        }

        log.debug("Telemetry subscription push interval value from broker: {}", intervalMs);
        return intervalMs;
    }

    public static boolean validateResourceLabel(Map<String, ?> m, String key) {
        if (!m.containsKey(key)) {
            log.trace("{} does not exist in map {}", key, m);
            return false;
        }

        if (m.get(key) == null) {
            log.trace("{} is null. map {}", key, m);
            return false;
        }

        if (!(m.get(key) instanceof String)) {
            log.trace("{} is not a string. map {}", key, m);
            return false;
        }

        String val = (String) m.get(key);
        if (val.isEmpty()) {
            log.trace("{} is empty string. value = {} map {}", key, val, m);
            return false;
        }
        return true;
    }

    public static boolean validateRequiredResourceLabels(Map<String, String> metadata) {
        return validateResourceLabel(metadata, MetricsContext.NAMESPACE);
    }

    public static CompressionType preferredCompressionType(List<CompressionType> acceptedCompressionTypes) {
        if (acceptedCompressionTypes != null && !acceptedCompressionTypes.isEmpty()) {
            // Broker is providing the compression types in order of preference. Grab the
            // first one.
            return acceptedCompressionTypes.get(0);
        }
        return CompressionType.NONE;
    }

    public static byte[] compress(byte[] raw, CompressionType compressionType) throws IOException {
        try (ByteBufferOutputStream compressedOut = new ByteBufferOutputStream(512)) {
            try (OutputStream out = compressionType.wrapForOutput(compressedOut, RecordBatch.CURRENT_MAGIC_VALUE)) {
                out.write(raw);
                out.flush();
            }
            compressedOut.buffer().flip();
            return Utils.toArray(compressedOut.buffer());
        }
    }

    public static ByteBuffer decompress(byte[] metrics, CompressionType compressionType) {
        ByteBuffer data = ByteBuffer.wrap(metrics);
        try (InputStream in = compressionType.wrapForInput(data, RecordBatch.CURRENT_MAGIC_VALUE, BufferSupplier.create());
            ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            byte[] bytes = new byte[data.capacity() * 2];
            int nRead;
            while ((nRead = in.read(bytes, 0, bytes.length)) != -1) {
                out.write(bytes, 0, nRead);
            }

            out.flush();
            return ByteBuffer.wrap(out.toByteArray());
        } catch (IOException e) {
            throw new KafkaException("Failed to decompress metrics data", e);
        }
    }

    public static MetricsData deserializeMetricsData(ByteBuffer serializedMetricsData) {
        try {
            return MetricsData.parseFrom(serializedMetricsData);
        } catch (IOException e) {
            throw new KafkaException("Unable to parse MetricsData payload", e);
        }
    }

    public static Uuid fetchClientInstanceId(ClientTelemetryReporter clientTelemetryReporter, Duration timeout) {
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("The timeout cannot be negative.");
        }

        Optional<Uuid> optionalUuid = clientTelemetryReporter.telemetrySender().clientInstanceId(timeout);
        return optionalUuid.orElse(null);
    }
}
