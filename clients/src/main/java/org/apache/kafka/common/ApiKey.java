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

package org.apache.kafka.common;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.Protocol;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Identifiers for Kafka APIs.
 *
 * This is a public API for both Kafka clients and internals.  Each ApiKey represents an API which
 * Kafka brokers understand.
 */
public enum ApiKey {
    // Metadata which is not intended to be exposed to end-users should go in
    // #{org.apache.kafka.common.protocol.ApiKeys} rather than here.  In order to maintain
    // compatibility, we can add more APIs to the list, but not remove or reorder existing APIs.
    PRODUCE("Produce"),
    FETCH("Fetch"),
    LIST_OFFSETS("Offsets"),
    METADATA("Metadata"),
    LEADER_AND_ISR("LeaderAndIsr"),
    STOP_REPLICA("StopReplica"),
    UPDATE_METADATA_KEY("UpdateMetadata"),
    CONTROLLED_SHUTDOWN_KEY("ControlledShutdown"),
    OFFSET_COMMIT("OffsetCommit"),
    OFFSET_FETCH("OffsetFetch"),
    FIND_COORDINATOR("FindCoordinator"),
    JOIN_GROUP("JoinGroup"),
    HEARTBEAT("Heartbeat"),
    LEAVE_GROUP("LeaveGroup"),
    SYNC_GROUP("SyncGroup"),
    DESCRIBE_GROUPS("DescribeGroups"),
    LIST_GROUPS("ListGroups"),
    SASL_HANDSHAKE("SaslHandshake"),
    API_VERSIONS("ApiVersions"),
    CREATE_TOPICS("CreateTopics"),
    DELETE_TOPICS("DeleteTopics"),
    DELETE_RECORDS("DeleteRecords"),
    INIT_PRODUCER_ID("InitProducerId"),
    OFFSET_FOR_LEADER_EPOCH("OffsetForLeaderEpoch"),
    ADD_PARTITIONS_TO_TXN("AddPartitionsToTxn"),
    ADD_OFFSETS_TO_TXN("AddOffsetsToTxn"),
    END_TXN("EndTxn"),
    WRITE_TXN_MARKERS("WriteTxnMarkers"),
    TXN_OFFSET_COMMIT("TxnOffsetCommit"),
    DESCRIBE_ACLS("DescribeAcls"),
    CREATE_ACLS("CreateAcls"),
    DELETE_ACLS("DeleteAcls"),
    DESCRIBE_CONFIGS("DescribeConfigs"),
    ALTER_CONFIGS("AlterConfigs");

    public static final List<ApiKey> VALUES =
        Collections.unmodifiableList(Arrays.asList(ApiKey.values()));

    private final String title;

    public static boolean hasId(short id) {
        return (id >= 0) && (id < VALUES.size());
    }

    public static ApiKey fromId(int id) {
        if ((id < 0) || (id >= VALUES.size())) {
            throw new IllegalArgumentException(String.format("Unexpected ApiKey id `%d`, " +
                "it should be between 0 and %d (inclusive)", id, VALUES.size() - 1));
        }
        return VALUES.get(id);
    }

    ApiKey(String title) {
        this.title = title;
    }

    /**
     * Get the numeric id fo this ApiKey.
     *
     * The numeric ID of an API is fixed.  It cannot be changed once the API is released.
     */
    public short id() {
        return (short) ordinal();
    }

    /**
     * Get the human-readable title of this ApiKey.
     *
     * The titles of an API can change over time.
     */
    public String title() {
        return title;
    }

    /**
     * Get the range of versions supported by this software.
     *
     * Note that for a Kafka client, this will represent the versions which the client itself
     * supports.  The brokers may support a different set of versions.
     *
     * @throws UnsupportedVersionException       If the software does not support any version of this API.
     * @return                                   The version range supported by this software,
     *                                           which could be different than the software you are
     *                                           communicating with over the network.
     */
    public ApiVersionRange supportedRange() {
        ApiVersionRange supportedRange = Protocol.supportedVersionRange(this);
        if (supportedRange == null)
            throw new UnsupportedVersionException("UNSUPPORTED");
        return supportedRange;
    }
}
