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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metadata.ZkMigrationStateRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.migration.ZkMigrationState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class ZkMigrationTest {

    private FeatureControlManager setupAndBootstrap(
        MetadataVersion metadataVersion,
        boolean emptyLog,
        boolean zkMigrationEnabled
    ) {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager featureControlManager = new FeatureControlManager.Builder()
            .setSnapshotRegistry(snapshotRegistry)
            .setMetadataVersion(metadataVersion)
            .build();

        List<ApiMessageAndVersion> records = new ArrayList<>();
        QuorumController.generateBootstrapRecords(
            LoggerFactory.getLogger(ZkMigrationTest.class),
            emptyLog,
            zkMigrationEnabled,
            BootstrapMetadata.fromVersion(metadataVersion, "test"),
            featureControlManager,
            records::add);
        RecordTestUtils.replayAll(featureControlManager, records);
        return featureControlManager;
    }

    private void verifyCannotBootstrapAgain(MetadataVersion metadataVersion, FeatureControlManager featureControl, Class<? extends Throwable> expectedException) {
        // Should not be able to produce another record in any circumstance after we've bootstrapped once
        assertThrows(expectedException, () ->
            QuorumController.generateBootstrapRecords(
                LoggerFactory.getLogger(ZkMigrationTest.class),
                false,
                true,
                BootstrapMetadata.fromVersion(metadataVersion, "test"),
                featureControl,
                record -> fail("Did not expect to get another record here, but got " + record + ". State was " + featureControl.zkMigrationState())));
    }

    @Test
    public void testBootstrapEmptyLog() {
        FeatureControlManager migrationControl;

        // In 3.3, we won't write any records
        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_3_IV0, true, true);
        assertEquals(ZkMigrationState.NONE, migrationControl.zkMigrationState());

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_3_IV0, true, false);
        assertEquals(ZkMigrationState.NONE, migrationControl.zkMigrationState());

        // In 3.4, we'll write something and should only be able to bootstrap once
        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_4_IV0, true, true);
        assertEquals(ZkMigrationState.PRE_MIGRATION, migrationControl.zkMigrationState());
        verifyCannotBootstrapAgain(MetadataVersion.IBP_3_4_IV0, migrationControl, IllegalStateException.class);

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_4_IV0, true, false);
        assertEquals(ZkMigrationState.NONE, migrationControl.zkMigrationState());
        verifyCannotBootstrapAgain(MetadataVersion.IBP_3_4_IV0, migrationControl, ConfigException.class);
    }

    @Test
    public void testBootstrapNonEmptyLog() {
        FeatureControlManager migrationControl;

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_3_IV0, false, true);
        assertEquals(ZkMigrationState.NONE, migrationControl.zkMigrationState());

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_3_IV0, false, false);
        assertEquals(ZkMigrationState.NONE, migrationControl.zkMigrationState());

        // Cannot have migrations enabled for a non-migrated KRaft cluster unless we're in a MIGRATION state
        assertThrows(ConfigException.class, () -> setupAndBootstrap(MetadataVersion.IBP_3_4_IV0, false, true));

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_4_IV0, false, false);
        assertEquals(ZkMigrationState.NONE, migrationControl.zkMigrationState());
    }

    @Test
    public void testNonMigrationStateTransitions() {
        // When migrations should not be present, ensure we can't change state from NONE
        FeatureControlManager migrationControl;
        assertThrows(ConfigException.class, () -> setupAndBootstrap(MetadataVersion.IBP_3_4_IV0, false, true));

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_4_IV0, true, false);
        assertEquals(ZkMigrationState.NONE, migrationControl.zkMigrationState());
        verifyCannotBootstrapAgain(MetadataVersion.IBP_3_4_IV0, migrationControl, ConfigException.class);
    }

    @Test
    public void testFailoverToNonMigrationControllerDuringMigration() {

    }

    @Test
    public void testFailoverToNonMigrationControllerAfterMigration() {

    }

    @Test
    public void testMigrationDisabledStateTransitions() {
        // When migrations are disabled, ensure we can't change state from NONE
        FeatureControlManager migrationControl;
        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_4_IV0, true, false);
        assertEquals(ZkMigrationState.NONE, migrationControl.zkMigrationState());

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_4_IV0, false, false);
        assertEquals(ZkMigrationState.NONE, migrationControl.zkMigrationState());
    }

    private FeatureControlManager verifyUpgradeFrom34(boolean migrationEnabled) {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        FeatureControlManager featureControl = new FeatureControlManager.Builder()
            .setSnapshotRegistry(snapshotRegistry)
            .build();

        // In 3.4, we only ever wrote PRE_MIGRATION (1)
        featureControl.replay(new ZkMigrationStateRecord().setZkMigrationState((byte) 1));
        assertEquals(featureControl.zkMigrationState(), ZkMigrationState.MIGRATION);
        assertFalse(featureControl.inPreMigrationMode(MetadataVersion.IBP_3_4_IV0));

        // Now bootstrap as if we're starting up in 3.5
        List<ApiMessageAndVersion> records = new ArrayList<>();
        QuorumController.generateBootstrapRecords(
            LoggerFactory.getLogger(ZkMigrationTest.class),
            false,
            true,
            BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_4_IV0, "test"),
            featureControl,
            records::add);

        records.forEach(record -> {
            featureControl.replay((ZkMigrationStateRecord) record.message());
        });
        return featureControl;
    }

    @Test
    public void testUpgradeFrom34MigrationEnabled() {
        FeatureControlManager migrationControl = verifyUpgradeFrom34(true);
        assertEquals(migrationControl.zkMigrationState(), ZkMigrationState.MIGRATION);
        assertFalse(migrationControl.inPreMigrationMode(MetadataVersion.IBP_3_4_IV0));
    }
}
