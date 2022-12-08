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

import java.util.Objects;

/**
 * Persistent state needed to recover an ongoing migration. This data is stored in ZooKeeper under the "/migration"
 * ZNode and is recovered by the active KRaft controller following an election. The absence of this data in ZK indicates
 * that no migration has been started.
 */
public class ZkMigrationLeadershipState {

    public static final ZkMigrationLeadershipState EMPTY = new ZkMigrationLeadershipState(-1, -1, -1, -1, -1, -1, -1);

    private final int kraftControllerId;

    private final int kraftControllerEpoch;

    private final long kraftMetadataOffset;

    private final int kraftMetadataEpoch;

    private final long lastUpdatedTimeMs;

    private final int migrationZkVersion;

    private final int controllerZkVersion;

    public ZkMigrationLeadershipState(int kraftControllerId, int kraftControllerEpoch,
                                      long kraftMetadataOffset, int kraftMetadataEpoch,
                                      long lastUpdatedTimeMs, int migrationZkVersion, int controllerZkVersion) {
        this.kraftControllerId = kraftControllerId;
        this.kraftControllerEpoch = kraftControllerEpoch;
        this.kraftMetadataOffset = kraftMetadataOffset;
        this.kraftMetadataEpoch = kraftMetadataEpoch;
        this.lastUpdatedTimeMs = lastUpdatedTimeMs;
        this.migrationZkVersion = migrationZkVersion;
        this.controllerZkVersion = controllerZkVersion;
    }

    public ZkMigrationLeadershipState withMigrationZkVersion(int zkVersion) {
        return new ZkMigrationLeadershipState(
            this.kraftControllerId, this.kraftControllerEpoch, this.kraftMetadataOffset,
            this.kraftMetadataEpoch, this.lastUpdatedTimeMs, zkVersion, this.controllerZkVersion);
    }

    public ZkMigrationLeadershipState withControllerZkVersion(int zkVersion) {
        return new ZkMigrationLeadershipState(
            this.kraftControllerId, this.kraftControllerEpoch, this.kraftMetadataOffset,
            this.kraftMetadataEpoch, this.lastUpdatedTimeMs, this.migrationZkVersion, zkVersion);
    }

    public ZkMigrationLeadershipState withNewKRaftController(int controllerId, int controllerEpoch) {
        return new ZkMigrationLeadershipState(
            controllerId, controllerEpoch, this.kraftMetadataOffset,
            this.kraftMetadataEpoch, this.lastUpdatedTimeMs, this.migrationZkVersion, this.controllerZkVersion);
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

    public long kraftMetadataEpoch() {
        return kraftMetadataEpoch;
    }

    public long lastUpdatedTimeMs() {
        return lastUpdatedTimeMs;
    }

    public int migrationZkVersion() {
        return migrationZkVersion;
    }

    public int controllerZkVersion() {
        return controllerZkVersion;
    }

    public boolean zkMigrationComplete() {
        return kraftMetadataOffset > 0;
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
            ", controllerZkVersion=" + controllerZkVersion +
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
            && controllerZkVersion == that.controllerZkVersion;
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
            controllerZkVersion);
    }
}
