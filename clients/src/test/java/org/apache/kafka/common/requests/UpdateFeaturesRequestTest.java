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
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UpdateFeaturesRequestTest {

    @Test
    public void testGetErrorResponse() {
        UpdateFeaturesRequestData.FeatureUpdateKeyCollection features =
            new UpdateFeaturesRequestData.FeatureUpdateKeyCollection();

        features.add(new UpdateFeaturesRequestData.FeatureUpdateKey()
            .setFeature("foo")
            .setMaxVersionLevel((short) 2)
        );

        features.add(new UpdateFeaturesRequestData.FeatureUpdateKey()
            .setFeature("bar")
            .setMaxVersionLevel((short) 3)
        );

        UpdateFeaturesRequest request = new UpdateFeaturesRequest(
            new UpdateFeaturesRequestData().setFeatureUpdates(features),
            UpdateFeaturesRequestData.HIGHEST_SUPPORTED_VERSION
        );

        UpdateFeaturesResponse response = request.getErrorResponse(0, new UnknownServerException());
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.topLevelError().error());
        assertEquals(0, response.data().results().size());
        assertEquals(Collections.singletonMap(Errors.UNKNOWN_SERVER_ERROR, 1), response.errorCounts());
    }

    @Test
    public void testUpdateFeaturesV0() {
        UpdateFeaturesRequestData.FeatureUpdateKeyCollection features =
                new UpdateFeaturesRequestData.FeatureUpdateKeyCollection();

        features.add(new UpdateFeaturesRequestData.FeatureUpdateKey()
            .setFeature("foo")
            .setMaxVersionLevel((short) 1)
            .setAllowDowngrade(true)
        );

        features.add(new UpdateFeaturesRequestData.FeatureUpdateKey()
            .setFeature("bar")
            .setMaxVersionLevel((short) 3)
        );

        UpdateFeaturesRequest request = new UpdateFeaturesRequest(
            new UpdateFeaturesRequestData().setFeatureUpdates(features),
            UpdateFeaturesRequestData.LOWEST_SUPPORTED_VERSION
        );
        ByteBuffer buffer = request.serialize();
        request = UpdateFeaturesRequest.parse(buffer, UpdateFeaturesRequestData.LOWEST_SUPPORTED_VERSION);

        List<UpdateFeaturesRequest.FeatureUpdateItem> updates = new ArrayList<>(request.featureUpdates());
        assertEquals(updates.size(), 2);
        assertEquals(updates.get(0).upgradeType(), FeatureUpdate.UpgradeType.SAFE_DOWNGRADE);
        assertEquals(updates.get(1).upgradeType(), FeatureUpdate.UpgradeType.UPGRADE);
    }

    @Test
    public void testUpdateFeaturesV1() {
        UpdateFeaturesRequestData.FeatureUpdateKeyCollection features =
            new UpdateFeaturesRequestData.FeatureUpdateKeyCollection();

        features.add(new UpdateFeaturesRequestData.FeatureUpdateKey()
            .setFeature("foo")
            .setMaxVersionLevel((short) 1)
            .setUpgradeType(FeatureUpdate.UpgradeType.SAFE_DOWNGRADE.code())
        );

        features.add(new UpdateFeaturesRequestData.FeatureUpdateKey()
            .setFeature("bar")
            .setMaxVersionLevel((short) 3)
        );

        UpdateFeaturesRequest request = new UpdateFeaturesRequest(
            new UpdateFeaturesRequestData().setFeatureUpdates(features),
            UpdateFeaturesRequestData.HIGHEST_SUPPORTED_VERSION
        );

        ByteBuffer buffer = request.serialize();
        request = UpdateFeaturesRequest.parse(buffer, UpdateFeaturesRequestData.HIGHEST_SUPPORTED_VERSION);

        List<UpdateFeaturesRequest.FeatureUpdateItem> updates = new ArrayList<>(request.featureUpdates());
        assertEquals(updates.size(), 2);
        assertEquals(updates.get(0).upgradeType(), FeatureUpdate.UpgradeType.SAFE_DOWNGRADE);
        assertEquals(updates.get(1).upgradeType(), FeatureUpdate.UpgradeType.UPGRADE);

    }

    @Test
    public void testUpdateFeaturesV1OldBoolean() {
        UpdateFeaturesRequestData.FeatureUpdateKeyCollection features =
            new UpdateFeaturesRequestData.FeatureUpdateKeyCollection();

        features.add(new UpdateFeaturesRequestData.FeatureUpdateKey()
            .setFeature("foo")
            .setMaxVersionLevel((short) 1)
            .setAllowDowngrade(true)
        );

        features.add(new UpdateFeaturesRequestData.FeatureUpdateKey()
            .setFeature("bar")
            .setMaxVersionLevel((short) 3)
        );

        UpdateFeaturesRequest request = new UpdateFeaturesRequest(
            new UpdateFeaturesRequestData().setFeatureUpdates(features),
            UpdateFeaturesRequestData.HIGHEST_SUPPORTED_VERSION
        );
        assertThrows(UnsupportedVersionException.class, request::serialize,
            "This should fail since allowDowngrade is not supported in v1 of this RPC");
    }

}
