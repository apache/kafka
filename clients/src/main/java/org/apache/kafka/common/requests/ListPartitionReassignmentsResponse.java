package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class ListPartitionReassignmentsResponse extends AbstractResponse {

    private final ListPartitionReassignmentsResponseData data;

    public ListPartitionReassignmentsResponse(Struct struct) {
        this(struct, ApiKeys.LIST_PARTITION_REASSIGNMENTS.latestVersion());
    }

    ListPartitionReassignmentsResponse(ListPartitionReassignmentsResponseData responseData) {
        this.data = responseData;
    }

    ListPartitionReassignmentsResponse(Struct struct, short version) {
        this.data = new ListPartitionReassignmentsResponseData(struct, version);
    }

    public static ListPartitionReassignmentsResponse parse(ByteBuffer buffer, short version) {
        return new ListPartitionReassignmentsResponse(ApiKeys.LIST_PARTITION_REASSIGNMENTS.responseSchema(version).read(buffer), version);
    }

    public ListPartitionReassignmentsResponseData data() {
        return data;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return true;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new HashMap<>();
        Errors topLevelErr = Errors.forCode(data.errorCode());
        counts.put(topLevelErr, counts.getOrDefault(topLevelErr, 0) + 1);

        return counts;
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }
}
