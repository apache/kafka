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

import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

}
