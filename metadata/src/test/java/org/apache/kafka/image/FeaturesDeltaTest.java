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

package org.apache.kafka.image;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.MetadataVersionTestUtils;

import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FeaturesDeltaTest {

    @Test
    public void testReplayWithUnsupportedFeatureLevel() {
        var featuresDelta = new FeaturesDelta(new FeaturesImage(emptyMap(), MetadataVersion.MINIMUM_VERSION));
        var exception = assertThrows(IllegalArgumentException.class, () -> featuresDelta.replay(new FeatureLevelRecord()
            .setName(MetadataVersion.FEATURE_NAME)
            .setFeatureLevel(MetadataVersionTestUtils.IBP_3_3_IV2_FEATURE_LEVEL)));
        assertTrue(exception.getMessage().contains("Unsupported metadata version - if you are currently upgrading your " +
            "cluster, please ensure the metadata version is set to " + MetadataVersion.MINIMUM_VERSION),
            "Expected substring missing from exception message: " + exception.getMessage());
    }

}
