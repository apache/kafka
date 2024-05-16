package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.StreamsPrepareAssignmentRequestData;
import org.apache.kafka.common.message.StreamsPrepareAssignmentResponseData;
import org.apache.kafka.common.message.StreamsPrepareAssignmentRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class StreamsPrepareAssignmentRequest extends AbstractRequest  {

    public static class Builder extends AbstractRequest.Builder<StreamsPrepareAssignmentRequest> {
        private final StreamsPrepareAssignmentRequestData data;

        public Builder(StreamsPrepareAssignmentRequestData data) {
            this(data, false);
        }

        public Builder(StreamsPrepareAssignmentRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.STREAMS_PREPARE_ASSIGNMENT, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public StreamsPrepareAssignmentRequest build(short version) {
            return new StreamsPrepareAssignmentRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final StreamsPrepareAssignmentRequestData data;

    public StreamsPrepareAssignmentRequest(StreamsPrepareAssignmentRequestData data, short version) {
        super(ApiKeys.STREAMS_PREPARE_ASSIGNMENT, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new StreamsPrepareAssignmentResponse(
            new StreamsPrepareAssignmentResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(Errors.forException(e).code())
        );
    }

    @Override
    public StreamsPrepareAssignmentRequestData data() {
        return data;
    }

    public static StreamsPrepareAssignmentRequest parse(ByteBuffer buffer, short version) {
        return new StreamsPrepareAssignmentRequest(new StreamsPrepareAssignmentRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
