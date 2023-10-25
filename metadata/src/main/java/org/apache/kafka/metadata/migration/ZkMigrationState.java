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

import org.apache.kafka.common.metadata.ZkMigrationStateRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.Optional;

/**
 * The cluster-wide ZooKeeper migration state.
 * </p>
 * An enumeration of the possible states of the ZkMigrationState field in ZkMigrationStateRecord.
 * This information is persisted in the metadata log and image.
 *
 * @see org.apache.kafka.common.metadata.ZkMigrationStateRecord
 */
public enum ZkMigrationState {
    /**
     * The cluster was created in KRaft mode. A cluster that was created in ZK mode can never attain
     * this state; the endpoint of migration is POST_MIGRATION, instead. This value is also used as
     * the default migration state in an empty metadata log.
     */
    NONE((byte) 0),

    /**
     * A KRaft controller has been elected with "zookeeper.metadata.migration.enable" set to "true".
     * The controller is now awaiting the preconditions for starting the migration to KRaft. In this
     * state, the metadata log does not yet contain the cluster's data. There is a metadata quorum,
     * but it is not doing anything useful yet.
     * </p>
     * In Kafka 3.4, PRE_MIGRATION was written out as value 1 to the log, but no MIGRATION state
     * was ever written. Since this would be an invalid log state in 3.5+, we have swapped the
     * enum values for PRE_MIGRATION and MIGRATION. This allows us to handle the upgrade case
     * from 3.4 without adding additional fields to the migration record.
     */
    PRE_MIGRATION((byte) 2),

    /**
     * The ZK data has been migrated, and the KRaft controller is now writing metadata to both ZK
     * and the metadata log. The controller will remain in this state until all the brokers have
     * been restarted in KRaft mode.
     */
    MIGRATION((byte) 1),

    /**
     * The migration from ZK has been fully completed. The cluster is running in KRaft mode. This state
     * will persist indefinitely after the migration. In operational terms, this is the same as the NONE
     * state.
     */
    POST_MIGRATION((byte) 3),

    /**
     * The controller is a ZK controller. No migration has been performed. This state is never persisted
     * and is only used by KafkaController in order to have a unified metric that indicates what kind of
     * metadata state the controller is in.
     */
    ZK((byte) 4);

    private final byte value;

    ZkMigrationState(byte value) {
        this.value = value;
    }

    public byte value() {
        return value;
    }

    public ApiMessageAndVersion toRecord() {
        return new ApiMessageAndVersion(
            new ZkMigrationStateRecord().setZkMigrationState(value()),
            (short) 0
        );
    }

    public static ZkMigrationState of(byte value) {
        return optionalOf(value)
            .orElseThrow(() -> new IllegalArgumentException(String.format("Value %s is not a valid Zk migration state", value)));
    }

    public static Optional<ZkMigrationState> optionalOf(byte value) {
        for (ZkMigrationState state : ZkMigrationState.values()) {
            if (state.value == value) {
                return Optional.of(state);
            }
        }
        return Optional.empty();
    }

    public boolean inProgress() {
        return this == PRE_MIGRATION || this == MIGRATION;
    }
}
