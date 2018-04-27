package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;

import java.util.LinkedHashMap;

public class SerializableFetchResponse extends AbstractFetchResponse<AbstractFetchResponse.SerializablePartitionData> {
    public SerializableFetchResponse(Errors error,
                                     LinkedHashMap<TopicPartition, SerializablePartitionData> responseData,
                                     int throttleTimeMs,
                                     int sessionId) {
        super(error, responseData, throttleTimeMs, sessionId);
    }
}
