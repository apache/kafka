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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FinalizedFeaturesTest {

    @Test
    public void testUnknownFeatures() {
        FinalizedFeatures features = FinalizedFeatures.unknown();

        assertTrue(features.isUnknown());
        assertTrue(features.finalizedFeatures().isEmpty());
        assertEquals(-1, features.finalizedFeaturesEpoch());
    }

    @Test
    public void testUnknownFeaturesMetadataVersionThrows() {
        FinalizedFeatures features = FinalizedFeatures.unknown();

        assertThrows(IllegalStateException.class, features::metadataVersionOrThrow);
    }

    @Test
    public void testUnknownFeaturesIsSingleton() {
        assertSame(FinalizedFeatures.unknown(), FinalizedFeatures.unknown());
    }

    @Test
    public void testFromKRaftVersion() {
        FinalizedFeatures features = FinalizedFeatures.fromMetadataVersion(MINIMUM_VERSION);

        assertFalse(features.isUnknown());
        assertEquals(MINIMUM_VERSION, features.metadataVersionOrThrow());
        assertEquals(MINIMUM_VERSION.featureLevel(), features.finalizedFeatures().get(FEATURE_NAME));
        assertEquals(1, features.finalizedFeatures().size());
        assertEquals(-1, features.finalizedFeaturesEpoch());
    }

    @Test
    public void testFromKRaftVersionNullThrows() {
        assertThrows(NullPointerException.class, () -> FinalizedFeatures.fromMetadataVersion(null));
    }

    @Test
    public void testKRaftModeFeatures() {
        FinalizedFeatures finalizedFeatures = FinalizedFeatures.of(MINIMUM_VERSION,
                Map.of("foo", (short) 2), 123);
        assertEquals(MINIMUM_VERSION.featureLevel(),
                finalizedFeatures.finalizedFeatures().get(FEATURE_NAME));
        assertEquals((short) 2,
                finalizedFeatures.finalizedFeatures().get("foo"));
        assertEquals(2, finalizedFeatures.finalizedFeatures().size());
    }

    @Test
    public void testOfNullMetadataVersionThrows() {
        assertThrows(NullPointerException.class,
            () -> FinalizedFeatures.of(null, Map.of(), 0));
    }

    @Test
    public void testOfNullFeaturesMapThrows() {
        assertThrows(NullPointerException.class,
            () -> FinalizedFeatures.of(MINIMUM_VERSION, null, 0));
    }

    @Test
    public void testSetFinalizedLevel() {
        FinalizedFeatures finalizedFeatures = FinalizedFeatures.of(
            MINIMUM_VERSION,
            Map.of("foo", (short) 2),
            123
        );

        FinalizedFeatures removedFeatures = finalizedFeatures.setFinalizedLevel("foo", (short) 0);
        assertNull(removedFeatures.finalizedFeatures().get("foo"));

        FinalizedFeatures sameFeatures = removedFeatures.setFinalizedLevel("foo", (short) 0);
        assertEquals(sameFeatures.finalizedFeatures(), removedFeatures.finalizedFeatures());
    }

    @Test
    public void testSetFinalizedLevelAdd() {
        FinalizedFeatures features = FinalizedFeatures.of(MINIMUM_VERSION, Map.of(), 123);

        FinalizedFeatures updatedFeatures = features.setFinalizedLevel("bar", (short) 5);
        assertEquals((short) 5, updatedFeatures.finalizedFeatures().get("bar"));
    }

    @Test
    public void testSetFinalizedLevelOnUnknownThrows() {
        assertThrows(IllegalStateException.class,
            () -> FinalizedFeatures.unknown().setFinalizedLevel("foo", (short) 1));
    }
}
