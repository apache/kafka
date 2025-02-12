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

import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FeaturesDeltaTest {

    @Test
    public void testReplayWithOldestFeatureLevelThatMayExistInTheLog() {
        var featuresDelta = new FeaturesDelta(new FeaturesImage(emptyMap(), MetadataVersion.MINIMUM_VERSION));
        featuresDelta.replay(new FeatureLevelRecord()
            .setName(MetadataVersion.FEATURE_NAME)
            .setFeatureLevel(MetadataVersionTestUtils.IBP_3_3_IV0_FEATURE_LEVEL));
        assertEquals(Map.of(), featuresDelta.changes());
    }

    @Test
    public void testReplayWithFeatureLevelThatWasNeverSupported() {
        var featuresDelta = new FeaturesDelta(new FeaturesImage(emptyMap(), MetadataVersion.MINIMUM_VERSION));
        assertThrows(IllegalArgumentException.class, () -> featuresDelta.replay(new FeatureLevelRecord()
            .setName(MetadataVersion.FEATURE_NAME)
            .setFeatureLevel(MetadataVersionTestUtils.IBP_3_2_IV0_FEATURE_LEVEL)));
    }

}
