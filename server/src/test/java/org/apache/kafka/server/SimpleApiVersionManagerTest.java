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
package org.apache.kafka.server;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.server.common.FinalizedFeatures;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleApiVersionManagerTest {

    @Test
    public void testUnknownFeaturesHasNoMetadataVersion() {
        SimpleApiVersionManager apiVersionManager = new SimpleApiVersionManager(
            ApiMessageType.ListenerType.CONTROLLER,
            true,
            FinalizedFeatures::unknown
        );
        ApiVersionsResponse response = apiVersionManager.apiVersionResponse(0, false);

        assertTrue(response.data().finalizedFeatures().isEmpty(),
            "Finalized features should be empty when no quorum exists");

        assertEquals(-1, response.data().finalizedFeaturesEpoch(),
            "Finalized features epoch should be -1 when no quorum exists");
    }
}
