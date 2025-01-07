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

package org.apache.kafka.image.publisher;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.loader.LogDeltaManifest;
import org.apache.kafka.image.loader.SnapshotManifest;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class ControllerRegistrationsPublisherTest {
    @Test
    public void testInitialControllers() {
        ControllerRegistrationsPublisher publisher = new ControllerRegistrationsPublisher();
        assertEquals(Collections.emptyMap(), publisher.controllers());
    }

    @Test
    public void testName() {
        ControllerRegistrationsPublisher publisher = new ControllerRegistrationsPublisher();
        assertEquals("ControllerRegistrationsPublisher", publisher.name());
    }

    private static final MetadataDelta TEST_DELTA;

    private static final MetadataImage TEST_IMAGE;

    private static final MetadataProvenance PROVENANCE = new MetadataProvenance(100L, 10, 2000L, true);

    static {
        TEST_DELTA = new MetadataDelta.Builder().build();
        TEST_DELTA.replay(new FeatureLevelRecord().
                setName(MetadataVersion.FEATURE_NAME).
                setFeatureLevel(MetadataVersion.IBP_3_6_IV2.featureLevel()));
        TEST_DELTA.replay(RecordTestUtils.createTestControllerRegistration(0, true));
        TEST_DELTA.replay(RecordTestUtils.createTestControllerRegistration(1, false));
        TEST_DELTA.replay(RecordTestUtils.createTestControllerRegistration(2, false));
        TEST_IMAGE = TEST_DELTA.apply(PROVENANCE);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testOnMetadataUpdate(boolean fromSnapshot) {
        ControllerRegistrationsPublisher publisher = new ControllerRegistrationsPublisher();
        if (fromSnapshot) {
            publisher.onMetadataUpdate(TEST_DELTA, TEST_IMAGE,
                new SnapshotManifest(new MetadataProvenance(100L, 10, 2000L, true), 100L));
        } else {
            publisher.onMetadataUpdate(TEST_DELTA, TEST_IMAGE,
                LogDeltaManifest.newBuilder().
                    provenance(PROVENANCE).
                    leaderAndEpoch(new LeaderAndEpoch(OptionalInt.of(1), 200)).
                    numBatches(3).
                    elapsedNs(1000L).
                    numBytes(234).
                    build());
        }
        System.out.println("TEST_IMAGE.cluster = " + TEST_IMAGE.cluster());
        assertEquals(new HashSet<>(Arrays.asList(0, 1, 2)), publisher.controllers().keySet());
        assertTrue(publisher.controllers().get(0).zkMigrationReady());
        assertFalse(publisher.controllers().get(1).zkMigrationReady());
        assertFalse(publisher.controllers().get(2).zkMigrationReady());
    }
}
