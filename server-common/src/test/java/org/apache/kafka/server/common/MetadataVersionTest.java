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

import org.apache.kafka.common.record.RecordVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.apache.kafka.server.common.MetadataVersion.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    public void testFromVersionString() {
        assertEquals(IBP_0_8_0, MetadataVersion.fromVersionString("0.8.0"));
        assertEquals(IBP_0_8_0, MetadataVersion.fromVersionString("0.8.0.0"));
        assertEquals(IBP_0_8_0, MetadataVersion.fromVersionString("0.8.0.1"));
        // should throw an exception as long as IBP_8_0_IV0 is not defined
        assertThrows(IllegalArgumentException.class, () -> MetadataVersion.fromVersionString("8.0"));

        assertEquals(IBP_0_8_1, MetadataVersion.fromVersionString("0.8.1"));
        assertEquals(IBP_0_8_1, MetadataVersion.fromVersionString("0.8.1.0"));
        assertEquals(IBP_0_8_1, MetadataVersion.fromVersionString("0.8.1.1"));

        assertEquals(IBP_0_8_2, MetadataVersion.fromVersionString("0.8.2"));
        assertEquals(IBP_0_8_2, MetadataVersion.fromVersionString("0.8.2.0"));
        assertEquals(IBP_0_8_2, MetadataVersion.fromVersionString("0.8.2.1"));

        assertEquals(IBP_0_9_0, MetadataVersion.fromVersionString("0.9.0"));
        assertEquals(IBP_0_9_0, MetadataVersion.fromVersionString("0.9.0.0"));
        assertEquals(IBP_0_9_0, MetadataVersion.fromVersionString("0.9.0.1"));

        assertEquals(IBP_0_10_0_IV0, MetadataVersion.fromVersionString("0.10.0-IV0"));

        assertEquals(IBP_0_10_0_IV1, MetadataVersion.fromVersionString("0.10.0"));
        assertEquals(IBP_0_10_0_IV1, MetadataVersion.fromVersionString("0.10.0.0"));
        assertEquals(IBP_0_10_0_IV1, MetadataVersion.fromVersionString("0.10.0.0-IV0"));
        assertEquals(IBP_0_10_0_IV1, MetadataVersion.fromVersionString("0.10.0.1"));

        assertEquals(IBP_0_10_1_IV0, MetadataVersion.fromVersionString("0.10.1-IV0"));
        assertEquals(IBP_0_10_1_IV1, MetadataVersion.fromVersionString("0.10.1-IV1"));

        assertEquals(IBP_0_10_1_IV2, MetadataVersion.fromVersionString("0.10.1"));
        assertEquals(IBP_0_10_1_IV2, MetadataVersion.fromVersionString("0.10.1.0"));
        assertEquals(IBP_0_10_1_IV2, MetadataVersion.fromVersionString("0.10.1-IV2"));
        assertEquals(IBP_0_10_1_IV2, MetadataVersion.fromVersionString("0.10.1.1"));

        assertEquals(IBP_0_10_2_IV0, MetadataVersion.fromVersionString("0.10.2"));
        assertEquals(IBP_0_10_2_IV0, MetadataVersion.fromVersionString("0.10.2.0"));
        assertEquals(IBP_0_10_2_IV0, MetadataVersion.fromVersionString("0.10.2-IV0"));
        assertEquals(IBP_0_10_2_IV0, MetadataVersion.fromVersionString("0.10.2.1"));

        assertEquals(IBP_0_11_0_IV0, MetadataVersion.fromVersionString("0.11.0-IV0"));
        assertEquals(IBP_0_11_0_IV1, MetadataVersion.fromVersionString("0.11.0-IV1"));

        assertEquals(IBP_0_11_0_IV2, MetadataVersion.fromVersionString("0.11.0"));
        assertEquals(IBP_0_11_0_IV2, MetadataVersion.fromVersionString("0.11.0.0"));
        assertEquals(IBP_0_11_0_IV2, MetadataVersion.fromVersionString("0.11.0-IV2"));
        assertEquals(IBP_0_11_0_IV2, MetadataVersion.fromVersionString("0.11.0.1"));

        assertEquals(IBP_1_0_IV0, MetadataVersion.fromVersionString("1.0"));
        assertEquals(IBP_1_0_IV0, MetadataVersion.fromVersionString("1.0.0"));
        assertEquals(IBP_1_0_IV0, MetadataVersion.fromVersionString("1.0.0-IV0"));
        assertEquals(IBP_1_0_IV0, MetadataVersion.fromVersionString("1.0.1"));
        assertThrows(IllegalArgumentException.class, () -> MetadataVersion.fromVersionString("0.1.0"));
        assertThrows(IllegalArgumentException.class, () -> MetadataVersion.fromVersionString("0.1.0.0"));
        assertThrows(IllegalArgumentException.class, () -> MetadataVersion.fromVersionString("0.1.0-IV0"));
        assertThrows(IllegalArgumentException.class, () -> MetadataVersion.fromVersionString("0.1.0.0-IV0"));

        assertEquals(IBP_1_1_IV0, MetadataVersion.fromVersionString("1.1-IV0"));

        assertEquals(IBP_2_0_IV1, MetadataVersion.fromVersionString("2.0"));
        assertEquals(IBP_2_0_IV0, MetadataVersion.fromVersionString("2.0-IV0"));
        assertEquals(IBP_2_0_IV1, MetadataVersion.fromVersionString("2.0-IV1"));

        assertEquals(IBP_2_1_IV2, MetadataVersion.fromVersionString("2.1"));
        assertEquals(IBP_2_1_IV0, MetadataVersion.fromVersionString("2.1-IV0"));
        assertEquals(IBP_2_1_IV1, MetadataVersion.fromVersionString("2.1-IV1"));
        assertEquals(IBP_2_1_IV2, MetadataVersion.fromVersionString("2.1-IV2"));

        assertEquals(IBP_2_2_IV1, MetadataVersion.fromVersionString("2.2"));
        assertEquals(IBP_2_2_IV0, MetadataVersion.fromVersionString("2.2-IV0"));
        assertEquals(IBP_2_2_IV1, MetadataVersion.fromVersionString("2.2-IV1"));

        assertEquals(IBP_2_3_IV1, MetadataVersion.fromVersionString("2.3"));
        assertEquals(IBP_2_3_IV0, MetadataVersion.fromVersionString("2.3-IV0"));
        assertEquals(IBP_2_3_IV1, MetadataVersion.fromVersionString("2.3-IV1"));

        assertEquals(IBP_2_4_IV1, MetadataVersion.fromVersionString("2.4"));
        assertEquals(IBP_2_4_IV0, MetadataVersion.fromVersionString("2.4-IV0"));
        assertEquals(IBP_2_4_IV1, MetadataVersion.fromVersionString("2.4-IV1"));

        assertEquals(IBP_2_5_IV0, MetadataVersion.fromVersionString("2.5"));
        assertEquals(IBP_2_5_IV0, MetadataVersion.fromVersionString("2.5-IV0"));

        assertEquals(IBP_2_6_IV0, MetadataVersion.fromVersionString("2.6"));
        assertEquals(IBP_2_6_IV0, MetadataVersion.fromVersionString("2.6-IV0"));

        assertEquals(IBP_2_7_IV0, MetadataVersion.fromVersionString("2.7-IV0"));
        assertEquals(IBP_2_7_IV1, MetadataVersion.fromVersionString("2.7-IV1"));
        assertEquals(IBP_2_7_IV2, MetadataVersion.fromVersionString("2.7-IV2"));

        assertEquals(IBP_2_8_IV1, MetadataVersion.fromVersionString("2.8"));
        assertEquals(IBP_2_8_IV0, MetadataVersion.fromVersionString("2.8-IV0"));
        assertEquals(IBP_2_8_IV1, MetadataVersion.fromVersionString("2.8-IV1"));

        assertEquals(IBP_3_0_IV1, MetadataVersion.fromVersionString("3.0"));
        assertEquals(IBP_3_0_IV0, MetadataVersion.fromVersionString("3.0-IV0"));
        assertEquals(IBP_3_0_IV1, MetadataVersion.fromVersionString("3.0-IV1"));

        assertEquals(IBP_3_1_IV0, MetadataVersion.fromVersionString("3.1"));
        assertEquals(IBP_3_1_IV0, MetadataVersion.fromVersionString("3.1-IV0"));

        assertEquals(IBP_3_2_IV0, MetadataVersion.fromVersionString("3.2"));
        assertEquals(IBP_3_2_IV0, MetadataVersion.fromVersionString("3.2-IV0"));

        assertEquals(IBP_3_3_IV0, MetadataVersion.fromVersionString("3.3-IV0"));
        assertEquals(IBP_3_3_IV1, MetadataVersion.fromVersionString("3.3-IV1"));
        assertEquals(IBP_3_3_IV2, MetadataVersion.fromVersionString("3.3-IV2"));
        assertEquals(IBP_3_3_IV3, MetadataVersion.fromVersionString("3.3-IV3"));

        assertEquals(IBP_3_4_IV0, MetadataVersion.fromVersionString("3.4-IV0"));

        assertEquals(IBP_3_5_IV0, MetadataVersion.fromVersionString("3.5-IV0"));
        assertEquals(IBP_3_5_IV1, MetadataVersion.fromVersionString("3.5-IV1"));
        assertEquals(IBP_3_5_IV2, MetadataVersion.fromVersionString("3.5-IV2"));

        assertEquals(IBP_3_6_IV0, MetadataVersion.fromVersionString("3.6-IV0"));
        assertEquals(IBP_3_6_IV1, MetadataVersion.fromVersionString("3.6-IV1"));
        assertEquals(IBP_3_6_IV2, MetadataVersion.fromVersionString("3.6-IV2"));

        assertEquals(IBP_3_7_IV0, MetadataVersion.fromVersionString("3.7-IV0"));
        assertEquals(IBP_3_7_IV1, MetadataVersion.fromVersionString("3.7-IV1"));
    }

    @Test
    public void testMinSupportedVersionFor() {
        assertEquals(IBP_0_8_0, MetadataVersion.minSupportedFor(RecordVersion.V0));
        assertEquals(IBP_0_10_0_IV0, MetadataVersion.minSupportedFor(RecordVersion.V1));
        assertEquals(IBP_0_11_0_IV0, MetadataVersion.minSupportedFor(RecordVersion.V2));

        // Ensure that all record versions have a defined min version so that we remember to update the method
        for (RecordVersion recordVersion : RecordVersion.values()) {
            assertNotNull(MetadataVersion.minSupportedFor(recordVersion));
        }
    }

    @Test
    public void testShortVersion() {
        assertEquals("0.8.0", IBP_0_8_0.shortVersion());
        assertEquals("0.10.0", IBP_0_10_0_IV0.shortVersion());
        assertEquals("0.10.0", IBP_0_10_0_IV1.shortVersion());
        assertEquals("0.11.0", IBP_0_11_0_IV0.shortVersion());
        assertEquals("0.11.0", IBP_0_11_0_IV1.shortVersion());
        assertEquals("0.11.0", IBP_0_11_0_IV2.shortVersion());
        assertEquals("1.0", IBP_1_0_IV0.shortVersion());
        assertEquals("1.1", IBP_1_1_IV0.shortVersion());
        assertEquals("2.0", IBP_2_0_IV0.shortVersion());
        assertEquals("2.0", IBP_2_0_IV1.shortVersion());
        assertEquals("2.1", IBP_2_1_IV0.shortVersion());
        assertEquals("2.1", IBP_2_1_IV1.shortVersion());
        assertEquals("2.1", IBP_2_1_IV2.shortVersion());
        assertEquals("2.2", IBP_2_2_IV0.shortVersion());
        assertEquals("2.2", IBP_2_2_IV1.shortVersion());
        assertEquals("2.3", IBP_2_3_IV0.shortVersion());
        assertEquals("2.3", IBP_2_3_IV1.shortVersion());
        assertEquals("2.4", IBP_2_4_IV0.shortVersion());
        assertEquals("2.5", IBP_2_5_IV0.shortVersion());
        assertEquals("2.6", IBP_2_6_IV0.shortVersion());
        assertEquals("2.7", IBP_2_7_IV2.shortVersion());
        assertEquals("2.8", IBP_2_8_IV0.shortVersion());
        assertEquals("2.8", IBP_2_8_IV1.shortVersion());
        assertEquals("3.0", IBP_3_0_IV0.shortVersion());
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
    }

    @Test
    public void testVersion() {
        assertEquals("0.8.0", IBP_0_8_0.version());
        assertEquals("0.8.2", IBP_0_8_2.version());
        assertEquals("0.10.0-IV0", IBP_0_10_0_IV0.version());
        assertEquals("0.10.0-IV1", IBP_0_10_0_IV1.version());
        assertEquals("0.11.0-IV0", IBP_0_11_0_IV0.version());
        assertEquals("0.11.0-IV1", IBP_0_11_0_IV1.version());
        assertEquals("0.11.0-IV2", IBP_0_11_0_IV2.version());
        assertEquals("1.0-IV0", IBP_1_0_IV0.version());
        assertEquals("1.1-IV0", IBP_1_1_IV0.version());
        assertEquals("2.0-IV0", IBP_2_0_IV0.version());
        assertEquals("2.0-IV1", IBP_2_0_IV1.version());
        assertEquals("2.1-IV0", IBP_2_1_IV0.version());
        assertEquals("2.1-IV1", IBP_2_1_IV1.version());
        assertEquals("2.1-IV2", IBP_2_1_IV2.version());
        assertEquals("2.2-IV0", IBP_2_2_IV0.version());
        assertEquals("2.2-IV1", IBP_2_2_IV1.version());
        assertEquals("2.3-IV0", IBP_2_3_IV0.version());
        assertEquals("2.3-IV1", IBP_2_3_IV1.version());
        assertEquals("2.4-IV0", IBP_2_4_IV0.version());
        assertEquals("2.5-IV0", IBP_2_5_IV0.version());
        assertEquals("2.6-IV0", IBP_2_6_IV0.version());
        assertEquals("2.7-IV2", IBP_2_7_IV2.version());
        assertEquals("2.8-IV0", IBP_2_8_IV0.version());
        assertEquals("2.8-IV1", IBP_2_8_IV1.version());
        assertEquals("3.0-IV0", IBP_3_0_IV0.version());
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
        assertTrue(MetadataVersion.checkIfMetadataChanged(IBP_3_2_IV0, IBP_3_0_IV0));
        assertTrue(MetadataVersion.checkIfMetadataChanged(IBP_3_2_IV0, IBP_2_8_IV1));
        assertTrue(MetadataVersion.checkIfMetadataChanged(IBP_3_3_IV1, IBP_3_3_IV0));

        // Check that argument order doesn't matter
        assertTrue(MetadataVersion.checkIfMetadataChanged(IBP_3_0_IV0, IBP_3_2_IV0));
        assertTrue(MetadataVersion.checkIfMetadataChanged(IBP_2_8_IV1, IBP_3_2_IV0));
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
    public void testIsElrSupported(MetadataVersion metadataVersion) {
        assertEquals(metadataVersion.equals(IBP_3_7_IV1),
                metadataVersion.isElrSupported());
        short expectPartitionRecordVersion = metadataVersion.equals(IBP_3_7_IV1) ? (short) 1 : (short) 0;
        assertEquals(expectPartitionRecordVersion, metadataVersion.partitionRecordVersion());
        short expectPartitionChangeRecordVersion = metadataVersion.equals(IBP_3_7_IV1) ? (short) 1 : (short) 0;
        assertEquals(expectPartitionChangeRecordVersion, metadataVersion.partitionChangeRecordVersion());
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class)
    public void testRegisterBrokerRecordVersion(MetadataVersion metadataVersion) {
        final short expectedVersion;
        if (metadataVersion.isAtLeast(MetadataVersion.IBP_3_4_IV0)) {
            expectedVersion = 2;
        } else if (metadataVersion.isAtLeast(IBP_3_3_IV3)) {
            expectedVersion = 1;
        } else {
            expectedVersion = 0;
        }
        assertEquals(expectedVersion, metadataVersion.registerBrokerRecordVersion());
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class)
    public void testGroupMetadataValueVersion(MetadataVersion metadataVersion) {
        final short expectedVersion;
        if (metadataVersion.isAtLeast(MetadataVersion.IBP_2_3_IV0)) {
            expectedVersion = 3;
        } else if (metadataVersion.isAtLeast(IBP_2_1_IV0)) {
            expectedVersion = 2;
        } else if (metadataVersion.isAtLeast(IBP_0_10_1_IV0)) {
            expectedVersion = 1;
        } else {
            expectedVersion = 0;
        }
        assertEquals(expectedVersion, metadataVersion.groupMetadataValueVersion());
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class)
    public void testOffsetCommitValueVersion(MetadataVersion metadataVersion) {
        final short expectedVersion;
        if (metadataVersion.isAtLeast(MetadataVersion.IBP_2_1_IV1)) {
            expectedVersion = 3;
        } else if (metadataVersion.isAtLeast(IBP_2_1_IV0)) {
            expectedVersion = 2;
        } else {
            expectedVersion = 1;
        }
        assertEquals(expectedVersion, metadataVersion.offsetCommitValueVersion(false));
    }

    @ParameterizedTest
    @EnumSource(value = MetadataVersion.class)
    public void testOffsetCommitValueVersionWithExpiredTimestamp(MetadataVersion metadataVersion) {
        assertEquals((short) 1, metadataVersion.offsetCommitValueVersion(true));
    }
}
