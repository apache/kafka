package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.UpdateFinalizedFeaturesResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

public class UpdateFinalizedFeaturesResponse extends AbstractResponse {

    public final UpdateFinalizedFeaturesResponseData data;

    public UpdateFinalizedFeaturesResponse(UpdateFinalizedFeaturesResponseData data) {
        this.data = data;
    }

    public UpdateFinalizedFeaturesResponse(Struct struct) {
        final short latestVersion = (short) (UpdateFinalizedFeaturesResponseData.SCHEMAS.length - 1);
        this.data = new UpdateFinalizedFeaturesResponseData(struct, latestVersion);
    }

    public UpdateFinalizedFeaturesResponse(Struct struct, short version) {
        this.data = new UpdateFinalizedFeaturesResponseData(struct, version);
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(Errors.forCode(data.errorCode()));
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public String toString() {
        return data.toString();
    }

    public UpdateFinalizedFeaturesResponseData data() {
        return data;
    }

    public static UpdateFinalizedFeaturesResponse parse(ByteBuffer buffer, short version) {
        return new UpdateFinalizedFeaturesResponse(ApiKeys.UPDATE_FINALIZED_FEATURES.parseResponse(version, buffer), version);
    }
}
