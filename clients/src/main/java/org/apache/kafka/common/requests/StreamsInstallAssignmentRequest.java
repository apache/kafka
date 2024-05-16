package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.StreamsInstallAssignmentRequestData;
import org.apache.kafka.common.message.StreamsInstallAssignmentResponseData;
import org.apache.kafka.common.message.StreamsInstallAssignmentRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class StreamsInstallAssignmentRequest extends AbstractRequest  {

    public static class Builder extends AbstractRequest.Builder<StreamsInstallAssignmentRequest> {
        private final StreamsInstallAssignmentRequestData data;

        public Builder(StreamsInstallAssignmentRequestData data) {
            this(data, false);
        }

        public Builder(StreamsInstallAssignmentRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.STREAMS_INSTALL_ASSIGNMENT, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public StreamsInstallAssignmentRequest build(short version) {
            return new StreamsInstallAssignmentRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final StreamsInstallAssignmentRequestData data;

    public StreamsInstallAssignmentRequest(StreamsInstallAssignmentRequestData data, short version) {
        super(ApiKeys.STREAMS_INSTALL_ASSIGNMENT, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new StreamsInstallAssignmentResponse(
            new StreamsInstallAssignmentResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(Errors.forException(e).code())
        );
    }

    @Override
    public StreamsInstallAssignmentRequestData data() {
        return data;
    }

    public static StreamsInstallAssignmentRequest parse(ByteBuffer buffer, short version) {
        return new StreamsInstallAssignmentRequest(new StreamsInstallAssignmentRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
