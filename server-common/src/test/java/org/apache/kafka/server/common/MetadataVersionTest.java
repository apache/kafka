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

import org.apache.kafka.common.protocol.ApiKeys;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.apache.kafka.server.common.MetadataVersion.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetadataVersionTest {
    @Test
    public void testKRaftFeatureLevelsBefore3_0_IV1() {
        for (int i = 0; i < MetadataVersion.IBP_3_0_IV1.ordinal(); i++) {
            assertEquals(-1, MetadataVersion.VERSIONS[i].featureLevel());
        }
    }

    @Test
    public void testKRaftFeatureLevelsAtAndAfter3_0_IV1() {
        for (int i = MetadataVersion.IBP_3_0_IV1.ordinal(); i < MetadataVersion.VERSIONS.length; i++) {
            int expectedLevel = i - MetadataVersion.IBP_3_0_IV1.ordinal() + 1;
            assertEquals(expectedLevel, MetadataVersion.VERSIONS[i].featureLevel());
        }
    }

    @Test
    @SuppressWarnings("checkstyle:JavaNCSS")
    public void testFromVersionString() {
        assertEquals(IBP_3_0_IV1, MetadataVersion.fromVersionString("3.0"));
        assertEquals(IBP_3_0_IV1, MetadataVersion.fromVersionString("3.0-IV1"));

        assertEquals(IBP_3_1_IV0, MetadataVersion.fromVersionString("3.1"));
        assertEquals(IBP_3_1_IV0, MetadataVersion.fromVersionString("3.1-IV0"));

        assertEquals(IBP_3_2_IV0, MetadataVersion.fromVersionString("3.2"));
        assertEquals(IBP_3_2_IV0, MetadataVersion.fromVersionString("3.2-IV0"));

        // 3.3-IV3 is the latest production version in the 3.3 line
        assertEquals(IBP_3_3_IV3, MetadataVersion.fromVersionString("3.3"));
        assertEquals(IBP_3_3_IV0, MetadataVersion.fromVersionString("3.3-IV0"));
        assertEquals(IBP_3_3_IV1, MetadataVersion.fromVersionString("3.3-IV1"));
        assertEquals(IBP_3_3_IV2, MetadataVersion.fromVersionString("3.3-IV2"));
        assertEquals(IBP_3_3_IV3, MetadataVersion.fromVersionString("3.3-IV3"));

        // 3.4-IV0 is the latest production version in the 3.4 line
        assertEquals(IBP_3_4_IV0, MetadataVersion.fromVersionString("3.4"));
        assertEquals(IBP_3_4_IV0, MetadataVersion.fromVersionString("3.4-IV0"));

        // 3.5-IV2 is the latest production version in the 3.5 line
        assertEquals(IBP_3_5_IV2, MetadataVersion.fromVersionString("3.5"));
        assertEquals(IBP_3_5_IV0, MetadataVersion.fromVersionString("3.5-IV0"));
        assertEquals(IBP_3_5_IV1, MetadataVersion.fromVersionString("3.5-IV1"));
        assertEquals(IBP_3_5_IV2, MetadataVersion.fromVersionString("3.5-IV2"));

        // 3.6-IV2 is the latest production version in the 3.6 line
        assertEquals(IBP_3_6_IV2, MetadataVersion.fromVersionString("3.6"));
        assertEquals(IBP_3_6_IV0, MetadataVersion.fromVersionString("3.6-IV0"));
        assertEquals(IBP_3_6_IV1, MetadataVersion.fromVersionString("3.6-IV1"));
        assertEquals(IBP_3_6_IV2, MetadataVersion.fromVersionString("3.6-IV2"));

        // 3.7-IV4 is the latest production version in the 3.7 line
        assertEquals(IBP_3_7_IV4, MetadataVersion.fromVersionString("3.7"));
        assertEquals(IBP_3_7_IV0, MetadataVersion.fromVersionString("3.7-IV0"));
        assertEquals(IBP_3_7_IV1, MetadataVersion.fromVersionString("3.7-IV1"));
        assertEquals(IBP_3_7_IV2, MetadataVersion.fromVersionString("3.7-IV2"));
        assertEquals(IBP_3_7_IV3, MetadataVersion.fromVersionString("3.7-IV3"));
        assertEquals(IBP_3_7_IV4, MetadataVersion.fromVersionString("3.7-IV4"));

        // 3.8-IV0 is the latest production version in the 3.8 line
        assertEquals(IBP_3_8_IV0, MetadataVersion.fromVersionString("3.8"));
        assertEquals(IBP_3_8_IV0, MetadataVersion.fromVersionString("3.8-IV0"));

        // 3.9-IV0 is the latest production version in the 3.9 line
        assertEquals(IBP_3_9_IV0, MetadataVersion.fromVersionString("3.9"));
        assertEquals(IBP_3_9_IV0, MetadataVersion.fromVersionString("3.9-IV0"));

        // 4.0-IV3 is the latest production version in the 4.0 line
        assertEquals(IBP_4_0_IV0, MetadataVersion.fromVersionString("4.0-IV0"));
        assertEquals(IBP_4_0_IV1, MetadataVersion.fromVersionString("4.0-IV1"));
        assertEquals(IBP_4_0_IV2, MetadataVersion.fromVersionString("4.0-IV2"));
        assertEquals(IBP_4_0_IV3, MetadataVersion.fromVersionString("4.0-IV3"));

        assertEquals(IBP_4_1_IV0, MetadataVersion.fromVersionString("4.1-IV0"));
    }

    @Test
    public void testShortVersion() {
        assertEquals("3.0", IBP_3_0_IV1.shortVersion());
        assertEquals("3.1", IBP_3_1_IV0.shortVersion());
        assertEquals("3.2", IBP_3_2_IV0.shortVersion());
        assertEquals("3.3", IBP_3_3_IV0.shortVersion());
        assertEquals("3.3", IBP_3_3_IV1.shortVersion());
        assertEquals("3.3", IBP_3_3_IV2.shortVersion());
        assertEquals("3.3", IBP_3_3_IV3.shortVersion());
        assertEquals("3.4", IBP_3_4_IV0.shortVersion());
        assertEquals("3.5", IBP_3_5_IV0.shortVersion());
        assertEquals("3.5", IBP_3_5_IV1.shortVersion());
        assertEquals("3.5", IBP_3_5_IV2.shortVersion());
        assertEquals("3.6", IBP_3_6_IV0.shortVersion());
        assertEquals("3.6", IBP_3_6_IV1.shortVersion());
        assertEquals("3.6", IBP_3_6_IV2.shortVersion());
        assertEquals("3.7", IBP_3_7_IV0.shortVersion());
        assertEquals("3.7", IBP_3_7_IV1.shortVersion());
        assertEquals("3.7", IBP_3_7_IV2.shortVersion());
        assertEquals("3.7", IBP_3_7_IV3.shortVersion());
        assertEquals("3.7", IBP_3_7_IV4.shortVersion());
        assertEquals("3.8", IBP_3_8_IV0.shortVersion());
        assertEquals("3.9", IBP_3_9_IV0.shortVersion());
        assertEquals("4.0", IBP_4_0_IV0.shortVersion());
        assertEquals("4.0", IBP_4_0_IV1.shortVersion());
        assertEquals("4.0", IBP_4_0_IV2.shortVersion());
        assertEquals("4.0", IBP_4_0_IV3.shortVersion());
        assertEquals("4.1", IBP_4_1_IV0.shortVersion());
    }

    @Test
    public void testVersion() {
        assertEquals("3.0-IV1", IBP_3_0_IV1.version());
        assertEquals("3.1-IV0", IBP_3_1_IV0.version());
        assertEquals("3.2-IV0", IBP_3_2_IV0.version());
        assertEquals("3.3-IV0", IBP_3_3_IV0.version());
        assertEquals("3.3-IV1", IBP_3_3_IV1.version());
        assertEquals("3.3-IV2", IBP_3_3_IV2.version());
        assertEquals("3.3-IV3", IBP_3_3_IV3.version());
        assertEquals("3.4-IV0", IBP_3_4_IV0.version());
        assertEquals("3.5-IV0", IBP_3_5_IV0.version());
        assertEquals("3.5-IV1", IBP_3_5_IV1.version());
        assertEquals("3.5-IV2", IBP_3_5_IV2.version());
        assertEquals("3.6-IV0", IBP_3_6_IV0.version());
        assertEquals("3.6-IV1", IBP_3_6_IV1.version());
        assertEquals("3.6-IV2", IBP_3_6_IV2.version());
        assertEquals("3.7-IV0", IBP_3_7_IV0.version());
        assertEquals("3.7-IV1", IBP_3_7_IV1.version());
        assertEquals("3.7-IV2", IBP_3_7_IV2.version());
        assertEquals("3.7-IV3", IBP_3_7_IV3.version());
        assertEquals("3.7-IV4", IBP_3_7_IV4.version());
        assertEquals("3.8-IV0", IBP_3_8_IV0.version());
        assertEquals("3.9-IV0", IBP_3_9_IV0.version());
        assertEquals("4.0-IV0", IBP_4_0_IV0.version());
        assertEquals("4.0-IV1", IBP_4_0_IV1.version());
        assertEquals("4.0-IV2", IBP_4_0_IV2.version());
        assertEquals("4.0-IV3", IBP_4_0_IV3.version());
        assertEquals("4.1-IV0", IBP_4_1_IV0.version());
    }

    @Test
    public void testPrevious() {
        for (int i = 1; i < MetadataVersion.VERSIONS.length - 2; i++) {
            MetadataVersion version = MetadataVersion.VERSIONS[i];
            assertTrue(version.previous().isPresent(), version.toString());
            assertEquals(MetadataVersion.VERSIONS[i - 1], version.previous().get());
        }
    }

    @Test
    public void testMetadataChanged() {
        assertFalse(MetadataVersion.checkIfMetadataChanged(IBP_3_2_IV0, IBP_3_2_IV0));
        assertTrue(MetadataVersion.checkIfMetadataChanged(IBP_3_2_IV0, IBP_3_1_IV0));
        assertTrue(MetadataVersion.checkIfMetadataChanged(IBP_3_2_IV0, IBP_3_0_IV1));
        assertTrue(MetadataVersion.checkIfMetadataChanged(IBP_3_2_IV0, IBP_3_0_IV1));
        assertTrue(MetadataVersion.checkIfMetadataChanged(IBP_3_3_IV1, IBP_3_3_IV0));

        // Check that argument order doesn't matter
        assertTrue(MetadataVersion.checkIfMetadataChanged(IBP_3_1_IV0, IBP_3_2_IV0));
        assertTrue(MetadataVersion.checkIfMetadataChanged(IBP_3_0_IV1, IBP_3_2_IV0));
    }

    @Test
    public void testKRaftVersions() {
        for (MetadataVersion metadataVersion : MetadataVersion.VERSIONS) {
            if (metadataVersion.isKRaftSupported()) {
                assertTrue(metadataVersion.featureLevel() > 0);
            } else {
                assertEquals(-1, metadataVersion.featureLevel());
            }
        }

        for (MetadataVersion metadataVersion : MetadataVersion.VERSIONS) {
            if (metadataVersion.isAtLeast(IBP_3_0_IV1)) {
                assertTrue(metadataVersion.isKRaftSupported(), metadataVersion.toString());
            } else {
                assertFalse(metadataVersion.isKRaftSupported());
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class)
    public void testIsInControlledShutdownStateSupported(MetadataVersion metadataVersion) {
        assertEquals(metadataVersion.isAtLeast(IBP_3_3_IV3),
            metadataVersion.isInControlledShutdownStateSupported());
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class)
    public void testIsDelegationTokenSupported(MetadataVersion metadataVersion) {
        assertEquals(metadataVersion.isAtLeast(IBP_3_6_IV2),
            metadataVersion.isDelegationTokenSupported());
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class)
    public void testDirectoryAssignmentSupported(MetadataVersion metadataVersion) {
        assertEquals(metadataVersion.isAtLeast(IBP_3_7_IV2), metadataVersion.isDirectoryAssignmentSupported());
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class)
    public void testIsElrSupported(MetadataVersion metadataVersion) {
        assertEquals(metadataVersion.isAtLeast(IBP_4_0_IV1), metadataVersion.isElrSupported());
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class)
    public void testPartitionRecordVersion(MetadataVersion metadataVersion) {
        final short expectedVersion;
        if (metadataVersion.isElrSupported()) {
            expectedVersion = (short) 2;
        } else if (metadataVersion.isDirectoryAssignmentSupported()) {
            expectedVersion = (short) 1;
        } else {
            expectedVersion = (short) 0;
        }
        assertEquals(expectedVersion, metadataVersion.partitionRecordVersion());
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class)
    public void testPartitionChangeRecordVersion(MetadataVersion metadataVersion) {
        final short expectedVersion;
        if (metadataVersion.isElrSupported()) {
            expectedVersion = (short) 2;
        } else if (metadataVersion.isDirectoryAssignmentSupported()) {
            expectedVersion = (short) 1;
        } else {
            expectedVersion = (short) 0;
        }
        assertEquals(expectedVersion, metadataVersion.partitionChangeRecordVersion());
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class)
    public void testRegisterBrokerRecordVersion(MetadataVersion metadataVersion) {
        final short expectedVersion;
        if (metadataVersion.isAtLeast(MetadataVersion.IBP_3_7_IV2)) {
            expectedVersion = 3;
        } else if (metadataVersion.isAtLeast(MetadataVersion.IBP_3_4_IV0)) {
            expectedVersion = 2;
        } else if (metadataVersion.isAtLeast(IBP_3_3_IV3)) {
            expectedVersion = 1;
        } else {
            expectedVersion = 0;
        }
        assertEquals(expectedVersion, metadataVersion.registerBrokerRecordVersion());
    }

    @Test
    public void assertLatestProductionIsLessThanLatest() {
        assertTrue(LATEST_PRODUCTION.ordinal() < MetadataVersion.latestTesting().ordinal(),
            "Expected LATEST_PRODUCTION " + LATEST_PRODUCTION +
            " to be less than the latest of " + MetadataVersion.latestTesting());
    }

    /**
     * We need to ensure that the latest production MV doesn't inadvertently rely on an unstable
     * request version. Currently, the broker selects the version for some inter-broker RPCs based on the MV 
     * rather than using the supported version from the ApiResponse.
     */
    @Test
    public void testProductionMetadataDontUseUnstableApiVersion() {
        MetadataVersion mv = MetadataVersion.latestProduction();
        assertTrue(mv.listOffsetRequestVersion() <= ApiKeys.LIST_OFFSETS.latestVersion(false));
        assertTrue(mv.fetchRequestVersion() <= ApiKeys.FETCH.latestVersion(false));
    }

    @Test
    public void assertLatestProductionIsProduction() {
        assertTrue(LATEST_PRODUCTION.isProduction());
    }

    @Test
    public void assertLatestIsNotProduction() {
        assertFalse(MetadataVersion.latestTesting().isProduction());
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class)
    public void testListOffsetsValueVersion(MetadataVersion metadataVersion) {
        final short expectedVersion = 10;
        if (metadataVersion.isAtLeast(IBP_4_0_IV3)) {
            assertEquals(expectedVersion, metadataVersion.listOffsetRequestVersion());
        } else {
            assertTrue(metadataVersion.listOffsetRequestVersion() < expectedVersion);
        }
    }
}
