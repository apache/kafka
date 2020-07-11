package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import org.apache.kafka.common.message.UpdateFinalizedFeaturesResponseData;
import org.apache.kafka.common.message.UpdateFinalizedFeaturesRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

public class UpdateFinalizedFeaturesRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<UpdateFinalizedFeaturesRequest> {

        private final UpdateFinalizedFeaturesRequestData data;

        public Builder(UpdateFinalizedFeaturesRequestData data) {
            super(ApiKeys.UPDATE_FINALIZED_FEATURES);
            this.data = data;
        }

        @Override
        public UpdateFinalizedFeaturesRequest build(short version) {
            return new UpdateFinalizedFeaturesRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public final UpdateFinalizedFeaturesRequestData data;

    public UpdateFinalizedFeaturesRequest(UpdateFinalizedFeaturesRequestData data, short version) {
        super(ApiKeys.UPDATE_FINALIZED_FEATURES, version);
        this.data = data;
    }

    public UpdateFinalizedFeaturesRequest(Struct struct, short version) {
        super(ApiKeys.UPDATE_FINALIZED_FEATURES, version);
        this.data = new UpdateFinalizedFeaturesRequestData(struct, version);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMsIgnored, Throwable e) {
        final ApiError apiError = ApiError.fromThrowable(e);
        return new UpdateFinalizedFeaturesResponse(
            new UpdateFinalizedFeaturesResponseData()
                .setErrorCode(apiError.error().code())
                .setErrorMessage(apiError.message()));
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    public UpdateFinalizedFeaturesRequestData data() {
        return data;
    }

    public static UpdateFinalizedFeaturesRequest parse(ByteBuffer buffer, short version) {
        return new UpdateFinalizedFeaturesRequest(
            ApiKeys.UPDATE_FINALIZED_FEATURES.parseRequest(version, buffer), version);
    }

    public static boolean isDeleteRequest(UpdateFinalizedFeaturesRequestData.FinalizedFeatureUpdateKey update) {
        return update.maxVersionLevel() < 1;
    }
}
