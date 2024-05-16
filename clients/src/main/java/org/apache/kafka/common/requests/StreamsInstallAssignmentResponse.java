package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.StreamsInstallAssignmentResponseData;
import org.apache.kafka.common.message.StreamsInitializeResponseData;
import org.apache.kafka.common.message.StreamsInstallAssignmentResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class StreamsInstallAssignmentResponse extends AbstractResponse {

    private final StreamsInstallAssignmentResponseData data;

    public StreamsInstallAssignmentResponse(StreamsInstallAssignmentResponseData data) {
        super(ApiKeys.STREAMS_INSTALL_ASSIGNMENT);
        this.data = data;
    }

    @Override
    public StreamsInstallAssignmentResponseData data() {
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

    public static StreamsInstallAssignmentResponse parse(ByteBuffer buffer, short version) {
        return new StreamsInstallAssignmentResponse(new StreamsInstallAssignmentResponseData(
            new ByteBufferAccessor(buffer), version));
    }
}
