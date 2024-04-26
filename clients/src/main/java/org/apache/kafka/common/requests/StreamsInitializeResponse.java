package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.StreamsInitializeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

/**
 * Possible error codes.
 *
 * - {@link Errors#GROUP_AUTHORIZATION_FAILED}
 * - {@link Errors#NOT_COORDINATOR}
 * - {@link Errors#COORDINATOR_NOT_AVAILABLE}
 * - {@link Errors#COORDINATOR_LOAD_IN_PROGRESS}
 * - {@link Errors#INVALID_REQUEST}
 * - {@link Errors#STREAMS_INVALID_TOPOLOGY}
 */
public class StreamsInitializeResponse extends AbstractResponse {

    private final StreamsInitializeResponseData data;

    public StreamsInitializeResponse(StreamsInitializeResponseData data) {
        super(ApiKeys.STREAMS_HEARTBEAT);
        this.data = data;
    }

    @Override
    public StreamsInitializeResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return Collections.singletonMap(Errors.forCode(data.errorCode()), 1);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public static StreamsInitializeResponse parse(ByteBuffer buffer, short version) {
        return new StreamsInitializeResponse(new StreamsInitializeResponseData(
            new ByteBufferAccessor(buffer), version));
    }
}
