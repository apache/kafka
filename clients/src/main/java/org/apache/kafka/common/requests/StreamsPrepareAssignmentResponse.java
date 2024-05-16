package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.StreamsPrepareAssignmentResponseData;
import org.apache.kafka.common.message.StreamsInstallAssignmentResponseData;
import org.apache.kafka.common.message.StreamsPrepareAssignmentResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
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
 * - {@link Errors#INVALID_GROUP_ID}
 * - {@link Errors#GROUP_ID_NOT_FOUND}
 * - {@link Errors#UNKNOWN_MEMBER_ID}
 * - {@link Errors#STALE_MEMBER_EPOCH}
 */
public class StreamsPrepareAssignmentResponse extends AbstractResponse {

    private final StreamsPrepareAssignmentResponseData data;

    public StreamsPrepareAssignmentResponse(StreamsPrepareAssignmentResponseData data) {
        super(ApiKeys.STREAMS_PREPARE_ASSIGNMENT);
        this.data = data;
    }

    @Override
    public StreamsPrepareAssignmentResponseData data() {
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

    public static StreamsPrepareAssignmentResponse parse(ByteBuffer buffer, short version) {
        return new StreamsPrepareAssignmentResponse(new StreamsPrepareAssignmentResponseData(
            new ByteBufferAccessor(buffer), version));
    }
    
}
