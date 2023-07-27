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
package org.apache.kafka.common.requests;

import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

public class UpdateFeaturesRequest extends AbstractRequest {

    public static class FeatureUpdateItem {
        private final String featureName;
        private final short featureLevel;
        private final FeatureUpdate.UpgradeType upgradeType;

        public FeatureUpdateItem(String featureName, short featureLevel, FeatureUpdate.UpgradeType upgradeType) {
            this.featureName = featureName;
            this.featureLevel = featureLevel;
            this.upgradeType = upgradeType;
        }

        public String feature() {
            return featureName;
        }

        public short versionLevel() {
            return featureLevel;
        }

        public FeatureUpdate.UpgradeType upgradeType() {
            return upgradeType;
        }

        public boolean isDeleteRequest() {
            return featureLevel < 1 && !upgradeType.equals(FeatureUpdate.UpgradeType.UPGRADE);
        }
    }

    public static class Builder extends AbstractRequest.Builder<UpdateFeaturesRequest> {

        private final UpdateFeaturesRequestData data;

        public Builder(UpdateFeaturesRequestData data) {
            super(ApiKeys.UPDATE_FEATURES);
            this.data = data;
        }

        @Override
        public UpdateFeaturesRequest build(short version) {
            return new UpdateFeaturesRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final UpdateFeaturesRequestData data;

    public UpdateFeaturesRequest(UpdateFeaturesRequestData data, short version) {
        super(ApiKeys.UPDATE_FEATURES, version);
        this.data = data;
    }

    public FeatureUpdateItem getFeature(String name) {
        UpdateFeaturesRequestData.FeatureUpdateKey update = data.featureUpdates().find(name);
        if (super.version() == 0) {
            if (update.allowDowngrade()) {
                return new FeatureUpdateItem(update.feature(), update.maxVersionLevel(), FeatureUpdate.UpgradeType.SAFE_DOWNGRADE);
            } else {
                return new FeatureUpdateItem(update.feature(), update.maxVersionLevel(), FeatureUpdate.UpgradeType.UPGRADE);
            }
        } else {
            return new FeatureUpdateItem(update.feature(), update.maxVersionLevel(), FeatureUpdate.UpgradeType.fromCode(update.upgradeType()));
        }
    }

    public Collection<FeatureUpdateItem> featureUpdates() {
        return data.featureUpdates().stream()
            .map(update -> getFeature(update.feature()))
            .collect(Collectors.toList());
    }

    @Override
    public UpdateFeaturesResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return UpdateFeaturesResponse.createWithErrors(
            ApiError.fromThrowable(e),
            Collections.emptyMap(),
            throttleTimeMs
        );
    }

    @Override
    public UpdateFeaturesRequestData data() {
        return data;
    }

    public static UpdateFeaturesRequest parse(ByteBuffer buffer, short version) {
        return new UpdateFeaturesRequest(new UpdateFeaturesRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
