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

package org.apache.kafka.server.common;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.kafka.server.common.MetadataVersion.FEATURE_NAME;
import static org.apache.kafka.server.common.MetadataVersion.MINIMUM_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class FinalizedFeaturesTest {
    @Test
    public void testKRaftModeFeatures() {
        FinalizedFeatures finalizedFeatures = new FinalizedFeatures(MINIMUM_VERSION,
                Map.of("foo", (short) 2), 123);
        assertEquals(MINIMUM_VERSION.featureLevel(),
                finalizedFeatures.finalizedFeatures().get(FEATURE_NAME));
        assertEquals((short) 2,
                finalizedFeatures.finalizedFeatures().get("foo"));
        assertEquals(2, finalizedFeatures.finalizedFeatures().size());
    }

    @Test
    public void testSetFinalizedLevel() {
        FinalizedFeatures finalizedFeatures = new FinalizedFeatures(
            MINIMUM_VERSION,
            Map.of("foo", (short) 2),
            123
        );

        // Override an existing finalized feature version to 0
        FinalizedFeatures removedFeatures = finalizedFeatures.setFinalizedLevel("foo", (short) 0);
        assertNull(removedFeatures.finalizedFeatures().get("foo"));

        // Override a missing finalized feature version to 0
        FinalizedFeatures sameFeatures = removedFeatures.setFinalizedLevel("foo", (short) 0);
        assertEquals(sameFeatures.finalizedFeatures(), removedFeatures.finalizedFeatures());
    }
}
