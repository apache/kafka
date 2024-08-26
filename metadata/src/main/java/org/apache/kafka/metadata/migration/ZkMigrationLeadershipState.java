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
package org.apache.kafka.metadata.migration;

import org.apache.kafka.raft.OffsetAndEpoch;

import java.util.Objects;

/**
 * Persistent state needed to recover an ongoing migration. This data is stored in ZooKeeper under the "/migration"
 * ZNode and is recovered by the active KRaft controller following an election. The absence of this data in ZK indicates
 * that no migration has been started.
 */
public class ZkMigrationLeadershipState {
    /**
     * A Kafka-internal constant used to indicate that the znode version is unknown. See ZkVersion.UnknownVersion.
     */
    public static final int UNKNOWN_ZK_VERSION = -2;

    // Use -2 as sentinel for "unknown version" for ZK versions to avoid sending an actual -1 "any version"
    // when doing ZK writes
    public static final ZkMigrationLeadershipState EMPTY =
        new ZkMigrationLeadershipState(-1, -1, -1, -1, -1, -2, -1, UNKNOWN_ZK_VERSION);

    private final int kraftControllerId;

    private final int kraftControllerEpoch;

    private final long kraftMetadataOffset;

    private final int kraftMetadataEpoch;

    private final long lastUpdatedTimeMs;

    private final int migrationZkVersion;

    private final int zkControllerEpoch;

    private final int zkControllerEpochZkVersion;


    public ZkMigrationLeadershipState(int kraftControllerId, int kraftControllerEpoch,
                                      long kraftMetadataOffset, int kraftMetadataEpoch,
                                      long lastUpdatedTimeMs, int migrationZkVersion,
                                      int zkControllerEpoch, int zkControllerEpochZkVersion) {
        this.kraftControllerId = kraftControllerId;
        this.kraftControllerEpoch = kraftControllerEpoch;
        this.kraftMetadataOffset = kraftMetadataOffset;
        this.kraftMetadataEpoch = kraftMetadataEpoch;
        this.lastUpdatedTimeMs = lastUpdatedTimeMs;
        this.migrationZkVersion = migrationZkVersion;
        this.zkControllerEpoch = zkControllerEpoch;
        this.zkControllerEpochZkVersion = zkControllerEpochZkVersion;
    }

    public ZkMigrationLeadershipState withMigrationZkVersion(int zkVersion) {
        return new ZkMigrationLeadershipState(
            this.kraftControllerId, this.kraftControllerEpoch, this.kraftMetadataOffset,
            this.kraftMetadataEpoch, this.lastUpdatedTimeMs, zkVersion, this.zkControllerEpoch, this.zkControllerEpochZkVersion);
    }

    public ZkMigrationLeadershipState withZkController(int zkControllerEpoch, int zkControllerEpochZkVersion) {
        return new ZkMigrationLeadershipState(
            this.kraftControllerId, this.kraftControllerEpoch, this.kraftMetadataOffset,
            this.kraftMetadataEpoch, this.lastUpdatedTimeMs, this.migrationZkVersion, zkControllerEpoch, zkControllerEpochZkVersion);
    }

    public ZkMigrationLeadershipState withUnknownZkController() {
        return withZkController(EMPTY.zkControllerEpoch, EMPTY.zkControllerEpochZkVersion);
    }


    public ZkMigrationLeadershipState withNewKRaftController(int controllerId, int controllerEpoch) {
        return new ZkMigrationLeadershipState(
            controllerId, controllerEpoch, this.kraftMetadataOffset,
            this.kraftMetadataEpoch, this.lastUpdatedTimeMs, this.migrationZkVersion, this.zkControllerEpoch, this.zkControllerEpochZkVersion);
    }

    public ZkMigrationLeadershipState withKRaftMetadataOffsetAndEpoch(long metadataOffset,
                                                                      int metadataEpoch) {
        return new ZkMigrationLeadershipState(
            this.kraftControllerId,
            this.kraftControllerEpoch,
            metadataOffset,
            metadataEpoch,
            this.lastUpdatedTimeMs,
            this.migrationZkVersion,
            this.zkControllerEpoch,
            this.zkControllerEpochZkVersion);
    }

    public int kraftControllerId() {
        return kraftControllerId;
    }

    public int kraftControllerEpoch() {
        return kraftControllerEpoch;
    }

    public long kraftMetadataOffset() {
        return kraftMetadataOffset;
    }

    public int kraftMetadataEpoch() {
        return kraftMetadataEpoch;
    }

    public long lastUpdatedTimeMs() {
        return lastUpdatedTimeMs;
    }

    public int migrationZkVersion() {
        return migrationZkVersion;
    }

    public int zkControllerEpoch() {
        return zkControllerEpoch;
    }

    public int zkControllerEpochZkVersion() {
        return zkControllerEpochZkVersion;
    }

    public boolean initialZkMigrationComplete() {
        return kraftMetadataOffset > 0;
    }

    public OffsetAndEpoch offsetAndEpoch() {
        return new OffsetAndEpoch(kraftMetadataOffset, kraftMetadataEpoch);
    }

    public boolean loggableChangeSinceState(ZkMigrationLeadershipState other) {
        if (other == null) {
            return false;
        }
        if (this.equals(other)) {
            return false;
        } else {
            // Did the controller change, or did we finish the migration?
            return
                this.kraftControllerId != other.kraftControllerId ||
                this.kraftControllerEpoch != other.kraftControllerEpoch ||
                (!other.initialZkMigrationComplete() && this.initialZkMigrationComplete());
        }
    }

    @Override
    public String toString() {
        return "ZkMigrationLeadershipState{" +
            "kraftControllerId=" + kraftControllerId +
            ", kraftControllerEpoch=" + kraftControllerEpoch +
            ", kraftMetadataOffset=" + kraftMetadataOffset +
            ", kraftMetadataEpoch=" + kraftMetadataEpoch +
            ", lastUpdatedTimeMs=" + lastUpdatedTimeMs +
            ", migrationZkVersion=" + migrationZkVersion +
            ", controllerZkEpoch=" + zkControllerEpoch +
            ", controllerZkVersion=" + zkControllerEpochZkVersion +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZkMigrationLeadershipState that = (ZkMigrationLeadershipState) o;
        return kraftControllerId == that.kraftControllerId
            && kraftControllerEpoch == that.kraftControllerEpoch
            && kraftMetadataOffset == that.kraftMetadataOffset
            && kraftMetadataEpoch == that.kraftMetadataEpoch
            && lastUpdatedTimeMs == that.lastUpdatedTimeMs
            && migrationZkVersion == that.migrationZkVersion
            && zkControllerEpoch == that.zkControllerEpoch
            && zkControllerEpochZkVersion == that.zkControllerEpochZkVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            kraftControllerId,
            kraftControllerEpoch,
            kraftMetadataOffset,
            kraftMetadataEpoch,
            lastUpdatedTimeMs,
            migrationZkVersion,
            zkControllerEpoch,
            zkControllerEpochZkVersion);
    }
}
