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

package org.apache.kafka.controller;

import org.apache.kafka.common.metadata.ZkMigrationStateRecord;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.migration.ZkMigrationState;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This class is for testing the log message or exception produced by ActivationRecordsGenerator. For tests that
 * verify the semantics of the returned records, see QuorumControllerTest.
 */
public class ActivationRecordsGeneratorTest {

    @Test
    public void testActivationMessageForEmptyLog() {
        ControllerResult<Void> result;
        result = ActivationRecordsGenerator.recordsForEmptyLog(
            logMsg -> assertEquals("Performing controller activation. The metadata log appears to be empty. " +
                "Appending 1 bootstrap record(s) at metadata.version 3.0-IV1 from bootstrap source 'test'.", logMsg),
            -1L,
            false,
            BootstrapMetadata.fromVersion(MetadataVersion.MINIMUM_BOOTSTRAP_VERSION, "test"),
            MetadataVersion.MINIMUM_KRAFT_VERSION
        );
        assertTrue(result.isAtomic());
        assertEquals(1, result.records().size());

        result = ActivationRecordsGenerator.recordsForEmptyLog(
            logMsg -> assertEquals("Performing controller activation. The metadata log appears to be empty. " +
                "Appending 1 bootstrap record(s) at metadata.version 3.4-IV0 from bootstrap " +
                "source 'test'. Setting the ZK migration state to NONE since this is a de-novo KRaft cluster.", logMsg),
            -1L,
            false,
            BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_4_IV0, "test"),
            MetadataVersion.IBP_3_4_IV0
        );
        assertTrue(result.isAtomic());
        assertEquals(2, result.records().size());


        result = ActivationRecordsGenerator.recordsForEmptyLog(
            logMsg -> assertEquals("Performing controller activation. The metadata log appears to be empty. " +
                "Appending 1 bootstrap record(s) at metadata.version 3.4-IV0 from bootstrap " +
                "source 'test'. Putting the controller into pre-migration mode. No metadata updates will be allowed " +
                "until the ZK metadata has been migrated.", logMsg),
            -1L,
            true,
            BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_4_IV0, "test"),
            MetadataVersion.IBP_3_4_IV0
        );
        assertTrue(result.isAtomic());
        assertEquals(2, result.records().size());

        assertEquals(
            "The bootstrap metadata.version 3.3-IV2 does not support ZK migrations. Cannot continue with ZK migrations enabled.",
            assertThrows(RuntimeException.class, () ->
                ActivationRecordsGenerator.recordsForEmptyLog(
                    logMsg -> fail(),
                    -1L,
                    true,
                    BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_3_IV2, "test"),
                    MetadataVersion.IBP_3_3_IV2
                )).getMessage()
        );

        result = ActivationRecordsGenerator.recordsForEmptyLog(
            logMsg -> assertEquals("Performing controller activation. The metadata log appears to be empty. " +
                "Appending 1 bootstrap record(s) in metadata transaction at metadata.version 3.6-IV1 from bootstrap " +
                "source 'test'. Setting the ZK migration state to NONE since this is a de-novo KRaft cluster.", logMsg),
            -1L,
            false,
            BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_6_IV1, "test"),
            MetadataVersion.IBP_3_6_IV1
        );
        assertFalse(result.isAtomic());
        assertEquals(4, result.records().size());

        result = ActivationRecordsGenerator.recordsForEmptyLog(
            logMsg -> assertEquals("Performing controller activation. The metadata log appears to be empty. " +
                "Appending 1 bootstrap record(s) in metadata transaction at metadata.version 3.6-IV1 from bootstrap " +
                "source 'test'. Putting the controller into pre-migration mode. No metadata updates will be allowed " +
                "until the ZK metadata has been migrated.", logMsg),
            -1L,
            true,
            BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_6_IV1, "test"),
            MetadataVersion.IBP_3_6_IV1
        );
        assertFalse(result.isAtomic());
        assertEquals(4, result.records().size());

        result = ActivationRecordsGenerator.recordsForEmptyLog(
            logMsg -> assertEquals("Performing controller activation. Aborting partial bootstrap records " +
                "transaction at offset 0. Re-appending 1 bootstrap record(s) in new metadata transaction at " +
                "metadata.version 3.6-IV1 from bootstrap source 'test'. Setting the ZK migration state to NONE " +
                "since this is a de-novo KRaft cluster.", logMsg),
            0L,
            false,
            BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_6_IV1, "test"),
            MetadataVersion.IBP_3_6_IV1
        );
        assertFalse(result.isAtomic());
        assertEquals(5, result.records().size());

        result = ActivationRecordsGenerator.recordsForEmptyLog(
            logMsg -> assertEquals("Performing controller activation. Aborting partial bootstrap records " +
                "transaction at offset 0. Re-appending 1 bootstrap record(s) in new metadata transaction at " +
                "metadata.version 3.6-IV1 from bootstrap source 'test'. Putting the controller into pre-migration " +
                "mode. No metadata updates will be allowed until the ZK metadata has been migrated.", logMsg),
            0L,
            true,
            BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_6_IV1, "test"),
            MetadataVersion.IBP_3_6_IV1
        );
        assertFalse(result.isAtomic());
        assertEquals(5, result.records().size());

        assertEquals(
            "Detected partial bootstrap records transaction at 0, but the metadata.version 3.6-IV0 does not " +
                "support transactions. Cannot continue.",
            assertThrows(RuntimeException.class, () ->
                ActivationRecordsGenerator.recordsForEmptyLog(
                    logMsg -> assertEquals("", logMsg),
                    0L,
                    true,
                    BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_6_IV0, "test"),
                    MetadataVersion.IBP_3_6_IV0
                )).getMessage()
        );
    }

    FeatureControlManager buildFeatureControl(
        MetadataVersion metadataVersion,
        Optional<ZkMigrationState> zkMigrationState
    ) {
        FeatureControlManager featureControl = new FeatureControlManager.Builder()
            .setMetadataVersion(metadataVersion).build();
        zkMigrationState.ifPresent(migrationState ->
            featureControl.replay((ZkMigrationStateRecord) migrationState.toRecord().message()));
        return featureControl;
    }

    @Test
    public void testActivationMessageForNonEmptyLogNoMigrations() {
        ControllerResult<Void> result;

        result = ActivationRecordsGenerator.recordsForNonEmptyLog(
            logMsg -> assertEquals("Performing controller activation. No metadata.version feature level " +
                "record was found in the log. Treating the log as version 3.0-IV1.", logMsg),
            -1L,
            false,
            buildFeatureControl(MetadataVersion.MINIMUM_KRAFT_VERSION, Optional.empty()),
            MetadataVersion.MINIMUM_KRAFT_VERSION
        );
        assertTrue(result.isAtomic());
        assertEquals(0, result.records().size());

        result = ActivationRecordsGenerator.recordsForNonEmptyLog(
            logMsg -> assertEquals("Performing controller activation.", logMsg),
            -1L,
            false,
            buildFeatureControl(MetadataVersion.IBP_3_3_IV0, Optional.empty()),
            MetadataVersion.IBP_3_3_IV0
        );
        assertTrue(result.isAtomic());
        assertEquals(0, result.records().size());

        result = ActivationRecordsGenerator.recordsForNonEmptyLog(
            logMsg -> assertEquals("Performing controller activation. Loaded ZK migration state of NONE.", logMsg),
            -1L,
            false,
            buildFeatureControl(MetadataVersion.IBP_3_4_IV0, Optional.empty()),
            MetadataVersion.IBP_3_4_IV0
        );
        assertTrue(result.isAtomic());
        assertEquals(0, result.records().size());

        result = ActivationRecordsGenerator.recordsForNonEmptyLog(
            logMsg -> assertEquals("Performing controller activation. Aborting in-progress metadata " +
                "transaction at offset 42. Loaded ZK migration state of NONE.", logMsg),
            42L,
            false,
            buildFeatureControl(MetadataVersion.IBP_3_6_IV1, Optional.empty()),
            MetadataVersion.IBP_3_6_IV1
        );
        assertTrue(result.isAtomic());
        assertEquals(1, result.records().size());

        assertEquals(
            "Detected in-progress transaction at offset 42, but the metadata.version 3.6-IV0 does not support " +
                "transactions. Cannot continue.",
            assertThrows(RuntimeException.class, () ->
                ActivationRecordsGenerator.recordsForNonEmptyLog(
                    logMsg -> fail(),
                    42L,
                    false,
                    buildFeatureControl(MetadataVersion.IBP_3_6_IV0, Optional.empty()),
                    MetadataVersion.IBP_3_6_IV0
                )).getMessage()
        );
    }

    @Test
    public void testActivationMessageForNonEmptyLogWithMigrations() {
        ControllerResult<Void> result;

        assertEquals(
            "Should not have ZK migrations enabled on a cluster running metadata.version 3.3-IV0",
            assertThrows(RuntimeException.class, () ->
                ActivationRecordsGenerator.recordsForNonEmptyLog(
                    logMsg -> fail(),
                    -1L,
                    true,
                    buildFeatureControl(MetadataVersion.IBP_3_3_IV0, Optional.empty()),
                    MetadataVersion.IBP_3_3_IV0
                )).getMessage()
        );

        assertEquals(
            "Should not have ZK migrations enabled on a cluster that was created in KRaft mode.",
            assertThrows(RuntimeException.class, () -> {
                ActivationRecordsGenerator.recordsForNonEmptyLog(
                    logMsg -> fail(),
                    -1L,
                    true,
                    buildFeatureControl(MetadataVersion.IBP_3_4_IV0, Optional.empty()),
                    MetadataVersion.IBP_3_4_IV0
                );
            }).getMessage()
        );

        result = ActivationRecordsGenerator.recordsForNonEmptyLog(
            logMsg -> assertEquals("Performing controller activation. Loaded ZK migration state of " +
                "PRE_MIGRATION. Activating pre-migration controller without empty log. There may be a partial " +
                "migration.", logMsg),
            -1L,
            true,
            buildFeatureControl(MetadataVersion.IBP_3_4_IV0, Optional.of(ZkMigrationState.PRE_MIGRATION)),
            MetadataVersion.IBP_3_4_IV0
        );
        assertTrue(result.isAtomic());
        assertEquals(0, result.records().size());

        result = ActivationRecordsGenerator.recordsForNonEmptyLog(
            logMsg -> assertEquals("Performing controller activation. Loaded ZK migration state of " +
                "PRE_MIGRATION.", logMsg),
            -1L,
            true,
            buildFeatureControl(MetadataVersion.IBP_3_6_IV1, Optional.of(ZkMigrationState.PRE_MIGRATION)),
            MetadataVersion.IBP_3_6_IV1
        );
        assertTrue(result.isAtomic());
        assertEquals(0, result.records().size());

        result = ActivationRecordsGenerator.recordsForNonEmptyLog(
            logMsg -> assertEquals("Performing controller activation. Loaded ZK migration state of MIGRATION. " +
                "Staying in ZK migration mode since 'zookeeper.metadata.migration.enable' is still 'true'.", logMsg),
            -1L,
            true,
            buildFeatureControl(MetadataVersion.IBP_3_4_IV0, Optional.of(ZkMigrationState.MIGRATION)),
            MetadataVersion.IBP_3_4_IV0
        );
        assertTrue(result.isAtomic());
        assertEquals(0, result.records().size());

        result = ActivationRecordsGenerator.recordsForNonEmptyLog(
            logMsg -> assertEquals("Performing controller activation. Loaded ZK migration state of MIGRATION. " +
                "Completing the ZK migration since this controller was configured with " +
                "'zookeeper.metadata.migration.enable' set to 'false'.", logMsg),
            -1L,
            false,
            buildFeatureControl(MetadataVersion.IBP_3_4_IV0, Optional.of(ZkMigrationState.MIGRATION)),
            MetadataVersion.IBP_3_4_IV0
        );
        assertTrue(result.isAtomic());
        assertEquals(1, result.records().size());

        result = ActivationRecordsGenerator.recordsForNonEmptyLog(
            logMsg -> assertEquals("Performing controller activation. Aborting in-progress metadata " +
                "transaction at offset 42. Loaded ZK migration state of MIGRATION. Completing the ZK migration " +
                "since this controller was configured with 'zookeeper.metadata.migration.enable' set to 'false'.", logMsg),
            42L,
            false,
            buildFeatureControl(MetadataVersion.IBP_3_6_IV1, Optional.of(ZkMigrationState.MIGRATION)),
            MetadataVersion.IBP_3_6_IV1
        );
        assertTrue(result.isAtomic());
        assertEquals(2, result.records().size());

        result = ActivationRecordsGenerator.recordsForNonEmptyLog(
            logMsg -> assertEquals("Performing controller activation. Loaded ZK migration state of " +
                "POST_MIGRATION.", logMsg),
            -1L,
            false,
            buildFeatureControl(MetadataVersion.IBP_3_4_IV0, Optional.of(ZkMigrationState.POST_MIGRATION)),
            MetadataVersion.IBP_3_4_IV0
        );
        assertTrue(result.isAtomic());
        assertEquals(0, result.records().size());

        result = ActivationRecordsGenerator.recordsForNonEmptyLog(
            logMsg -> assertEquals("Performing controller activation. Aborting in-progress metadata " +
                "transaction at offset 42. Loaded ZK migration state of POST_MIGRATION.", logMsg),
            42L,
            false,
            buildFeatureControl(MetadataVersion.IBP_3_6_IV1, Optional.of(ZkMigrationState.POST_MIGRATION)),
            MetadataVersion.IBP_3_6_IV1
        );
        assertTrue(result.isAtomic());
        assertEquals(1, result.records().size());

        result = ActivationRecordsGenerator.recordsForNonEmptyLog(
            logMsg -> assertEquals("Performing controller activation. Loaded ZK migration state of " +
                "POST_MIGRATION. Ignoring 'zookeeper.metadata.migration.enable' value of 'true' since the " +
                "ZK migration has been completed.", logMsg),
            -1L,
            true,
            buildFeatureControl(MetadataVersion.IBP_3_4_IV0, Optional.of(ZkMigrationState.POST_MIGRATION)),
            MetadataVersion.IBP_3_4_IV0
        );
        assertTrue(result.isAtomic());
        assertEquals(0, result.records().size());

        result = ActivationRecordsGenerator.recordsForNonEmptyLog(
            logMsg -> assertEquals("Performing controller activation. Aborting in-progress metadata " +
                "transaction at offset 42. Loaded ZK migration state of POST_MIGRATION. Ignoring " +
                "'zookeeper.metadata.migration.enable' value of 'true' since the ZK migration has been completed.", logMsg),
            42L,
            true,
            buildFeatureControl(MetadataVersion.IBP_3_6_IV1, Optional.of(ZkMigrationState.POST_MIGRATION)),
            MetadataVersion.IBP_3_6_IV1
        );
        assertTrue(result.isAtomic());
        assertEquals(1, result.records().size());
    }
}
