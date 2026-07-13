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
package org.apache.kafka.network;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.EnvelopeResponseData;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.DescribeQuorumResponse;
import org.apache.kafka.common.requests.EnvelopeResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.RequestAndSize;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.network.metrics.RequestChannelMetrics;
import org.apache.kafka.network.metrics.RequestMetrics;
import org.apache.kafka.server.config.AbstractKafkaConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

public final class Request implements BaseRequest {
    private static final Logger LOG = LoggerFactory.getLogger(Request.class);
    private static final Logger REQUEST_LOGGER = LoggerFactory.getLogger("kafka.request.logger");

    private final int processor;
    private final RequestContext context;
    private final long startTimeNanos;
    private final MemoryPool memoryPool;
    private final Session session;

    private volatile ByteBuffer buffer;
    private final RequestChannelMetrics metrics;
    private final Optional<Request> envelope;
    private final RequestAndSize bodyAndSize;
    private final JsonNode requestLog;

    // These need to be volatile because the readers are in the network thread and the writers are in the request
    // handler threads or the purgatory threads
    private volatile long requestDequeueTimeNanos = -1L;
    private volatile long apiLocalCompleteTimeNanos = -1L;
    private volatile long responseCompleteTimeNanos = -1L;
    private volatile long responseDequeueTimeNanos = -1L;
    private volatile long messageConversionsTimeNanos = 0L;
    private volatile long apiThrottleTimeMs = 0L;
    private volatile long temporaryMemoryBytes = 0L;

    private volatile OptionalLong callbackRequestDequeueTimeNanos = OptionalLong.empty();
    private volatile OptionalLong callbackRequestCompleteTimeNanos = OptionalLong.empty();
    private volatile LongConsumer recordNetworkThreadTimeCallback = __ -> { };

    private static boolean isRequestLoggingEnabled(RequestHeader header) {
        return REQUEST_LOGGER.isDebugEnabled()
                || (REQUEST_LOGGER.isInfoEnabled() && header.isApiVersionDeprecated());
    }

    public Request(int processor, RequestContext context, long startTimeNanos,
                   MemoryPool memoryPool, ByteBuffer buffer,
                   RequestChannelMetrics metrics, Optional<Request> envelope) {
        this.processor = processor;
        this.context = context;
        this.startTimeNanos = startTimeNanos;
        this.memoryPool = memoryPool;
        this.buffer = buffer;
        this.metrics = metrics;
        this.envelope = envelope;

        this.session = new Session(context.principal, context.clientAddress);
        this.bodyAndSize = context.parseRequest(buffer);

        // This is constructed on creation of a Request so that the JSON representation is computed before the request is
        // processed by the api layer. Otherwise, a ProduceRequest can occur without its data (ie. it goes into purgatory).
        this.requestLog = isRequestLoggingEnabled(context.header)
            ? RequestConvertToJson.request(loggableRequest())
            : null;

        // Most request types are parsed entirely into objects at this point. For those we can release the underlying
        // buffer. Some (like produce, or any time the schema contains fields of types BYTES or NULLABLE_BYTES) retain
        // a reference to the buffer. For those requests we cannot release the buffer early, but only when request
        // processing is done.
        if (!header().apiKey().requiresDelayedAllocation) {
            releaseBuffer();
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Processor {} received request: {}", processor, requestDesc(true));
        }
    }

    public Request(int processor, RequestContext context, long startTimeNanos,
                   MemoryPool memoryPool, ByteBuffer buffer,
                   RequestChannelMetrics metrics) {
        this(processor, context, startTimeNanos, memoryPool, buffer, metrics, Optional.empty());
    }

    public int processor() {
        return processor;
    }

    public RequestContext context() {
        return context;
    }

    public long startTimeNanos() {
        return startTimeNanos;
    }

    public MemoryPool memoryPool() {
        return memoryPool;
    }

    public Session session() {
        return session;
    }

    public long requestDequeueTimeNanos() {
        return requestDequeueTimeNanos;
    }

    public void requestDequeueTimeNanos(long requestDequeueTimeNanos) {
        this.requestDequeueTimeNanos = requestDequeueTimeNanos;
    }

    public long apiLocalCompleteTimeNanos() {
        return apiLocalCompleteTimeNanos;
    }

    public void apiLocalCompleteTimeNanos(long apiLocalCompleteTimeNanos) {
        this.apiLocalCompleteTimeNanos = apiLocalCompleteTimeNanos;
    }

    public void responseCompleteTimeNanos(long responseCompleteTimeNanos) {
        this.responseCompleteTimeNanos = responseCompleteTimeNanos;
    }

    public void responseDequeueTimeNanos(long responseDequeueTimeNanos) {
        this.responseDequeueTimeNanos = responseDequeueTimeNanos;
    }

    public long messageConversionsTimeNanos() {
        return messageConversionsTimeNanos;
    }

    public void messageConversionsTimeNanos(long messageConversionsTimeNanos) {
        this.messageConversionsTimeNanos = messageConversionsTimeNanos;
    }

    public void apiThrottleTimeMs(long apiThrottleTimeMs) {
        this.apiThrottleTimeMs = apiThrottleTimeMs;
    }

    public long temporaryMemoryBytes() {
        return temporaryMemoryBytes;
    }

    public void temporaryMemoryBytes(long temporaryMemoryBytes) {
        this.temporaryMemoryBytes = temporaryMemoryBytes;
    }

    public OptionalLong callbackRequestDequeueTimeNanos() {
        return callbackRequestDequeueTimeNanos;
    }

    public void callbackRequestDequeueTimeNanos(OptionalLong callbackRequestDequeueTimeNanos) {
        this.callbackRequestDequeueTimeNanos = callbackRequestDequeueTimeNanos;
    }

    public OptionalLong callbackRequestCompleteTimeNanos() {
        return callbackRequestCompleteTimeNanos;
    }

    public void callbackRequestCompleteTimeNanos(OptionalLong callbackRequestCompleteTimeNanos) {
        this.callbackRequestCompleteTimeNanos = callbackRequestCompleteTimeNanos;
    }

    public RequestHeader header() {
        return context.header;
    }

    private int sizeOfBodyInBytes() {
        return bodyAndSize.size;
    }

    public int sizeInBytes() {
        return header().size() + sizeOfBodyInBytes();
    }

    public boolean isForwarded() {
        return envelope.isPresent();
    }

    public Optional<Request> envelope() {
        return envelope;
    }

    public Optional<JsonNode> requestLog() {
        return Optional.ofNullable(requestLog);
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    private boolean shouldReturnNotController(AbstractResponse response) {
        if (response instanceof DescribeQuorumResponse) {
            return response.errorCounts().containsKey(Errors.NOT_LEADER_OR_FOLLOWER);
        }
        return response.errorCounts().containsKey(Errors.NOT_CONTROLLER);
    }

    public Send buildResponseSend(AbstractResponse abstractResponse) {
        if (envelope.isPresent()) {
            EnvelopeResponse envelopeResponse;
            if (shouldReturnNotController(abstractResponse)) {
                envelopeResponse = new EnvelopeResponse(new EnvelopeResponseData()
                    .setErrorCode(Errors.NOT_CONTROLLER.code()));
            } else {
                ByteBuffer responseBytes = context.buildResponseEnvelopePayload(abstractResponse);
                envelopeResponse = new EnvelopeResponse(responseBytes, Errors.NONE);
            }
            return envelope.get().context.buildResponseSend(envelopeResponse);
        }
        return context.buildResponseSend(abstractResponse);
    }

    public Optional<JsonNode> responseNode(AbstractResponse response) {
        if (isRequestLoggingEnabled(context.header)) {
            return Optional.of(RequestConvertToJson.response(response, context.apiVersion()));
        }
        return Optional.empty();
    }

    public RequestHeader headerForLoggingOrThrottling() {
        if (envelope.isPresent()) {
            return envelope.get().context.header;
        }
        return context.header;
    }

    public String requestDesc(boolean details) {
        String forwardDescription = envelope.map(request -> "Forwarded request: " + request.context + " ").orElse("");
        return forwardDescription + header() + " -- " + loggableRequest().toString(details);
    }

    public <T extends AbstractRequest> T body(Class<T> clazz) {
        if (clazz.isInstance(bodyAndSize.request)) {
            return clazz.cast(bodyAndSize.request);
        }
        throw new ClassCastException("Expected request with type " + clazz.getName()
            + ", but found " + bodyAndSize.request.getClass().getName());
    }

    public AbstractRequest loggableRequest() {
        if (bodyAndSize.request instanceof AlterConfigsRequest alterConfigs) {
            var newData = alterConfigs.data().duplicate();
            newData.resources().forEach(resource -> {
                ConfigResource.Type resourceType = ConfigResource.Type.forId(resource.resourceType());
                resource.configs().forEach(config ->
                    config.setValue(AbstractKafkaConfig.loggableValue(resourceType, config.name(), config.value()))
                );
            });
            return new AlterConfigsRequest(newData, alterConfigs.version());
        } else if (bodyAndSize.request instanceof IncrementalAlterConfigsRequest alterConfigs) {
            var newData = alterConfigs.data().duplicate();
            newData.resources().forEach(resource -> {
                ConfigResource.Type resourceType = ConfigResource.Type.forId(resource.resourceType());
                resource.configs().forEach(config ->
                    config.setValue(AbstractKafkaConfig.loggableValue(resourceType, config.name(), config.value()))
                );
            });
            return new IncrementalAlterConfigsRequest.Builder(newData).build(alterConfigs.version());
        }
        return bodyAndSize.request;
    }

    public long requestThreadTimeNanos() {
        if (apiLocalCompleteTimeNanos == -1L) {
            apiLocalCompleteTimeNanos = Time.SYSTEM.nanoseconds();
        }
        return Math.max(apiLocalCompleteTimeNanos - requestDequeueTimeNanos, 0L);
    }

    /**
     * Converts nanos to millis with micros precision as additional decimal places in the request log have low
     * signal to noise ratio. When it comes to metrics, there is little difference either way as we round the value
     * to the nearest long.
     */
    private static double nanosToMs(long nanos) {
        long positiveNanos = Math.max(nanos, 0);
        return (double) TimeUnit.NANOSECONDS.toMicros(positiveNanos) / 1000;
    }

    public void updateRequestMetrics(long networkThreadTimeNanos, Optional<JsonNode> responseLog) {
        long endTimeNanos = Time.SYSTEM.nanoseconds();

        double requestQueueTimeMs = nanosToMs(requestDequeueTimeNanos - startTimeNanos);
        long cbCompleteNanos = callbackRequestCompleteTimeNanos.orElse(0L);
        long cbDequeueNanos = callbackRequestDequeueTimeNanos.orElse(0L);
        long callbackRequestTimeNanos = cbCompleteNanos - cbDequeueNanos;
        double apiLocalTimeMs = nanosToMs(apiLocalCompleteTimeNanos - requestDequeueTimeNanos + callbackRequestTimeNanos);
        double apiRemoteTimeMs = nanosToMs(responseCompleteTimeNanos - apiLocalCompleteTimeNanos - callbackRequestTimeNanos);
        double responseQueueTimeMs = nanosToMs(responseDequeueTimeNanos - responseCompleteTimeNanos);
        double responseSendTimeMs = nanosToMs(endTimeNanos - responseDequeueTimeNanos);
        double messageConversionsTimeMs = nanosToMs(messageConversionsTimeNanos);
        double totalTimeMs = nanosToMs(endTimeNanos - startTimeNanos);

        List<String> overrideMetricNames = switch (header().apiKey()) {
            case FETCH -> {
                String specifiedMetricName = body(FetchRequest.class).isFromFollower()
                        ? RequestMetrics.FOLLOW_FETCH_METRIC_NAME
                        : RequestMetrics.CONSUMER_FETCH_METRIC_NAME;
                yield List.of(specifiedMetricName, ApiKeys.FETCH.name);
            }

            case ADD_PARTITIONS_TO_TXN -> body(AddPartitionsToTxnRequest.class).allVerifyOnlyRequest()
                    ? List.of(RequestMetrics.VERIFY_PARTITIONS_IN_TXN_METRIC_NAME)
                    : List.of(ApiKeys.ADD_PARTITIONS_TO_TXN.name);

            case LIST_CONFIG_RESOURCES -> header().apiVersion() == 0
                    ? List.of(RequestMetrics.LIST_CLIENT_METRICS_RESOURCES_METRIC_NAME, ApiKeys.LIST_CONFIG_RESOURCES.name)
                    : List.of(ApiKeys.LIST_CONFIG_RESOURCES.name);

            default -> List.of(header().apiKey().name);
        };

        for (String metricName : overrideMetricNames) {
            RequestMetrics m = metrics.get(metricName);
            m.requestRate(header().apiVersion()).mark();
            m.deprecatedRequestRate(header().apiKey(), header().apiVersion(), context.clientInformation)
                .ifPresent(Meter::mark);
            m.requestQueueTimeHist.update(Math.round(requestQueueTimeMs));
            m.localTimeHist.update(Math.round(apiLocalTimeMs));
            m.remoteTimeHist.update(Math.round(apiRemoteTimeMs));
            m.throttleTimeHist.update(apiThrottleTimeMs);
            m.responseQueueTimeHist.update(Math.round(responseQueueTimeMs));
            m.responseSendTimeHist.update(Math.round(responseSendTimeMs));
            m.totalTimeHist.update(Math.round(totalTimeMs));
            m.requestBytesHist.update(sizeOfBodyInBytes());
            m.messageConversionsTimeHist.ifPresent(h -> h.update(Math.round(messageConversionsTimeMs)));
            m.tempMemoryBytesHist.ifPresent(h -> h.update(temporaryMemoryBytes));
        }

        // Records network handler thread usage. This is included towards the request quota for the
        // user/client. Throttling is only performed when request handler thread usage
        // is recorded, just before responses are queued for delivery.
        // The time recorded here is the time spent on the network thread for receiving this request
        // and sending the response. Note that for the first request on a connection, the time includes
        // the total time spent on authentication, which may be significant for SASL/SSL.
        recordNetworkThreadTimeCallback.accept(networkThreadTimeNanos);

        if (isRequestLoggingEnabled(header())) {
            JsonNode desc = RequestConvertToJson.requestDescMetrics(header(), requestLog(), responseLog,
                context, session, isForwarded(),
                totalTimeMs, requestQueueTimeMs, apiLocalTimeMs,
                apiRemoteTimeMs, apiThrottleTimeMs, responseQueueTimeMs,
                responseSendTimeMs, temporaryMemoryBytes,
                messageConversionsTimeMs);
            String logPrefix = "Completed request:{}";
            if (header().isApiVersionDeprecated()) {
                REQUEST_LOGGER.info(logPrefix, desc);
            } else {
                REQUEST_LOGGER.debug(logPrefix, desc);
            }
        }
    }

    public void releaseBuffer() {
        if (envelope.isPresent()) {
            envelope.get().releaseBuffer();
        } else {
            if (buffer != null) {
                memoryPool.release(buffer);
                buffer = null;
            }
        }
    }

    public void setRecordNetworkThreadTimeCallback(LongConsumer callback) {
        this.recordNetworkThreadTimeCallback = callback;
    }

    @Override
    public String toString() {
        return "Request(processor=" + processor
            + ", connectionId=" + context.connectionId
            + ", session=" + session
            + ", listenerName=" + context.listenerName
            + ", securityProtocol=" + context.securityProtocol
            + ", buffer=" + buffer
            + ", envelope=" + envelope + ")";
    }
}
