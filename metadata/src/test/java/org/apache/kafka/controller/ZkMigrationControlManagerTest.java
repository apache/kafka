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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.migration.ZkMigrationState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class ZkMigrationControlManagerTest {
    public static class NoOpZkMigrationBootstrap implements ZkMigrationBootstrap {
        @Override
        public void bootstrapInitialMigrationState(
            MetadataVersion metadataVersion,
            boolean isMetadataLogEmpty,
            Consumer<ApiMessageAndVersion> recordConsumer
        ) {

        }
    }

    private ZkMigrationControlManager setupAndBootstrap(
        MetadataVersion metadataVersion,
        boolean emptyLog,
        boolean zkMigrationEnabled
    ) {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        ZkMigrationControlManager migrationControl = new ZkMigrationControlManager(
            snapshotRegistry, new LogContext(), zkMigrationEnabled
        );
        List<ApiMessageAndVersion> records = new ArrayList<>();
        migrationControl.bootstrapInitialMigrationState(metadataVersion, emptyLog, records::add);
        records.forEach(record -> {
            migrationControl.replay((ZkMigrationStateRecord) record.message());
        });
        return migrationControl;
    }

    private void verifyCannotBootstrapAgain(MetadataVersion metadataVersion, ZkMigrationControlManager migrationControl) {
        // Should not be able to produce another record in any circumstance after we've bootstrapped once
        try {
            migrationControl.bootstrapInitialMigrationState(metadataVersion, false,
                record -> fail("Did not expect to get another record here, but got " + record + ". State was " + migrationControl.zkMigrationState()));
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "Detected an in-progress migration during startup, cannot continue.");
        }
    }

    @Test
    public void testBootstrapEmptyLog() {
        ZkMigrationControlManager migrationControl;

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_3_IV0, true, true);
        assertEquals(ZkMigrationState.UNINITIALIZED, migrationControl.zkMigrationState());
        verifyCannotBootstrapAgain(MetadataVersion.IBP_3_3_IV0, migrationControl);

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_3_IV0, true, false);
        assertEquals(ZkMigrationState.UNINITIALIZED, migrationControl.zkMigrationState());
        verifyCannotBootstrapAgain(MetadataVersion.IBP_3_3_IV0, migrationControl);

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_4_IV0, true, true);
        assertEquals(ZkMigrationState.PRE_MIGRATION, migrationControl.zkMigrationState());
        verifyCannotBootstrapAgain(MetadataVersion.IBP_3_4_IV0, migrationControl);

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_4_IV0, true, false);
        assertEquals(ZkMigrationState.NONE, migrationControl.zkMigrationState());
        verifyCannotBootstrapAgain(MetadataVersion.IBP_3_4_IV0, migrationControl);
    }

    @Test
    public void testBootstrapNonEmptyLog() {
        ZkMigrationControlManager migrationControl;

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_3_IV0, false, true);
        assertEquals(ZkMigrationState.UNINITIALIZED, migrationControl.zkMigrationState());

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_3_IV0, false, false);
        assertEquals(ZkMigrationState.UNINITIALIZED, migrationControl.zkMigrationState());

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_4_IV0, false, true);
        assertEquals(ZkMigrationState.NONE, migrationControl.zkMigrationState());

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_4_IV0, false, false);
        assertEquals(ZkMigrationState.NONE, migrationControl.zkMigrationState());
    }

    @Test
    public void testMigrationStateTransitions() {
        ZkMigrationControlManager migrationControl;

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_4_IV0, true, true);
        assertEquals(ZkMigrationState.PRE_MIGRATION, migrationControl.zkMigrationState());

        assertThrows(IllegalStateException.class, () ->
            migrationControl.replay(new ZkMigrationStateRecord().setZkMigrationState(ZkMigrationState.PRE_MIGRATION.value())));
        assertThrows(IllegalStateException.class, () ->
            migrationControl.replay(new ZkMigrationStateRecord().setZkMigrationState(ZkMigrationState.NONE.value())));
        assertThrows(IllegalStateException.class, () ->
            migrationControl.replay(new ZkMigrationStateRecord().setZkMigrationState(ZkMigrationState.POST_MIGRATION.value())));

        migrationControl.replay(new ZkMigrationStateRecord().setZkMigrationState(ZkMigrationState.MIGRATION.value()));
        assertEquals(ZkMigrationState.MIGRATION, migrationControl.zkMigrationState());

        assertThrows(IllegalStateException.class, () ->
            migrationControl.replay(new ZkMigrationStateRecord().setZkMigrationState(ZkMigrationState.PRE_MIGRATION.value())));
        assertThrows(IllegalStateException.class, () ->
            migrationControl.replay(new ZkMigrationStateRecord().setZkMigrationState(ZkMigrationState.NONE.value())));
        assertThrows(IllegalStateException.class, () ->
            migrationControl.replay(new ZkMigrationStateRecord().setZkMigrationState(ZkMigrationState.MIGRATION.value())));

        migrationControl.replay(new ZkMigrationStateRecord().setZkMigrationState(ZkMigrationState.POST_MIGRATION.value()));
        assertEquals(ZkMigrationState.POST_MIGRATION, migrationControl.zkMigrationState());

        verifyNoStateTransitionAllowed(migrationControl);
    }

    private void verifyNoStateTransitionAllowed(ZkMigrationControlManager migrationControl) {
        assertThrows(IllegalStateException.class, () ->
            migrationControl.replay(new ZkMigrationStateRecord().setZkMigrationState(ZkMigrationState.PRE_MIGRATION.value())));
        assertThrows(IllegalStateException.class, () ->
            migrationControl.replay(new ZkMigrationStateRecord().setZkMigrationState(ZkMigrationState.NONE.value())));
        assertThrows(IllegalStateException.class, () ->
            migrationControl.replay(new ZkMigrationStateRecord().setZkMigrationState(ZkMigrationState.MIGRATION.value())));
        assertThrows(IllegalStateException.class, () ->
            migrationControl.replay(new ZkMigrationStateRecord().setZkMigrationState(ZkMigrationState.POST_MIGRATION.value())));
    }

    @Test
    public void testNonMigrationStateTransitions() {
        // When migrations should not be present, ensure we can't change state from NONE
        ZkMigrationControlManager migrationControl;
        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_4_IV0, false, true);
        assertEquals(ZkMigrationState.NONE, migrationControl.zkMigrationState());
        verifyNoStateTransitionAllowed(migrationControl);

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_4_IV0, true, false);
        assertEquals(ZkMigrationState.NONE, migrationControl.zkMigrationState());
        verifyNoStateTransitionAllowed(migrationControl);
    }

    @Test
    public void testMigrationDisabledStateTransitions() {
        // When migrations are disabled, ensure we can't change state from NONE
        ZkMigrationControlManager migrationControl;
        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_4_IV0, true, false);
        assertEquals(ZkMigrationState.NONE, migrationControl.zkMigrationState());
        verifyNoStateTransitionAllowed(migrationControl);

        migrationControl = setupAndBootstrap(MetadataVersion.IBP_3_4_IV0, false, false);
        assertEquals(ZkMigrationState.NONE, migrationControl.zkMigrationState());
        verifyNoStateTransitionAllowed(migrationControl);
    }
}
