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
package org.apache.kafka.common.protocol;

import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

/**
 * Identifiers for all the Kafka APIs
 */
public enum ApiKeys {
    PRODUCE(0, "Produce", false),
    FETCH(1, "Fetch", false),
    LIST_OFFSETS(2, "Offsets", false),
    METADATA(3, "Metadata", false),
    LEADER_AND_ISR(4, "LeaderAndIsr", true),
    STOP_REPLICA(5, "StopReplica", true),
    UPDATE_METADATA_KEY(6, "UpdateMetadata", true),
    CONTROLLED_SHUTDOWN_KEY(7, "ControlledShutdown", true),
    OFFSET_COMMIT(8, "OffsetCommit", false),
    OFFSET_FETCH(9, "OffsetFetch", false),
    FIND_COORDINATOR(10, "FindCoordinator", false),
    JOIN_GROUP(11, "JoinGroup", false),
    HEARTBEAT(12, "Heartbeat", false),
    LEAVE_GROUP(13, "LeaveGroup", false),
    SYNC_GROUP(14, "SyncGroup", false),
    DESCRIBE_GROUPS(15, "DescribeGroups", false),
    LIST_GROUPS(16, "ListGroups", false),
    SASL_HANDSHAKE(17, "SaslHandshake", false),
    API_VERSIONS(18, "ApiVersions", false) {
        @Override
        public Struct parseResponse(short version, ByteBuffer buffer) {
            // Fallback to version 0 for ApiVersions response. If a client sends an ApiVersionsRequest
            // using a version higher than that supported by the broker, a version 0 response is sent
            // to the client indicating UNSUPPORTED_VERSION.
            return parseResponse(version, buffer, (short) 0);
        }
    },
    CREATE_TOPICS(19, "CreateTopics", false),
    DELETE_TOPICS(20, "DeleteTopics", false),
    DELETE_RECORDS(21, "DeleteRecords", false),
    INIT_PRODUCER_ID(22, "InitProducerId", false),
    OFFSET_FOR_LEADER_EPOCH(23, "OffsetForLeaderEpoch", true),
    ADD_PARTITIONS_TO_TXN(24, "AddPartitionsToTxn", false),
    ADD_OFFSETS_TO_TXN(25, "AddOffsetsToTxn", false),
    END_TXN(26, "EndTxn", false),
    WRITE_TXN_MARKERS(27, "WriteTxnMarkers", true),
    TXN_OFFSET_COMMIT(28, "TxnOffsetCommit", false),
    DESCRIBE_ACLS(29, "DescribeAcls", false),
    CREATE_ACLS(30, "CreateAcls", false),
    DELETE_ACLS(31, "DeleteAcls", false),
    DESCRIBE_CONFIGS(32, "DescribeConfigs", false),
    ALTER_CONFIGS(33, "AlterConfigs", false);

    private static final ApiKeys[] ID_TO_TYPE;
    private static final int MIN_API_KEY = 0;
    public static final int MAX_API_KEY;

    static {
        int maxKey = -1;
        for (ApiKeys key : ApiKeys.values())
            maxKey = Math.max(maxKey, key.id);
        ApiKeys[] idToType = new ApiKeys[maxKey + 1];
        for (ApiKeys key : ApiKeys.values())
            idToType[key.id] = key;
        ID_TO_TYPE = idToType;
        MAX_API_KEY = maxKey;
    }

    /** the permanent and immutable id of an API--this can't change ever */
    public final short id;

    /** an english description of the api--this is for debugging and can change */
    public final String name;

    /** indicates if this is a ClusterAction request used only by brokers */
    public final boolean clusterAction;

    ApiKeys(int id, String name, boolean clusterAction) {
        if (id < 0)
            throw new IllegalArgumentException("id must not be negative, id: " + id);
        this.id = (short) id;
        this.name = name;
        this.clusterAction = clusterAction;
    }

    public static ApiKeys forId(int id) {
        if (!hasId(id))
            throw new IllegalArgumentException(String.format("Unexpected ApiKeys id `%s`, it should be between `%s` " +
                    "and `%s` (inclusive)", id, MIN_API_KEY, MAX_API_KEY));
        return ID_TO_TYPE[id];
    }

    public static boolean hasId(int id) {
        return id >= MIN_API_KEY && id <= MAX_API_KEY;
    }

    public short latestVersion() {
        if (id >= Protocol.CURR_VERSION.length)
            throw new IllegalArgumentException("Latest version for API key " + this + " is not defined");
        return Protocol.CURR_VERSION[id];
    }

    public short oldestVersion() {
        if (id >= Protocol.MIN_VERSIONS.length)
            throw new IllegalArgumentException("Oldest version for API key " + this + " is not defined");
        return Protocol.MIN_VERSIONS[id];
    }

    public Schema requestSchema(short version) {
        return schemaFor(Protocol.REQUESTS, version);
    }

    public Schema responseSchema(short version) {
        return schemaFor(Protocol.RESPONSES, version);
    }

    public Struct parseRequest(short version, ByteBuffer buffer) {
        return requestSchema(version).read(buffer);
    }

    public Struct parseResponse(short version, ByteBuffer buffer) {
        return responseSchema(version).read(buffer);
    }

    protected Struct parseResponse(short version, ByteBuffer buffer, short fallbackVersion) {
        int bufferPosition = buffer.position();
        try {
            return responseSchema(version).read(buffer);
        } catch (SchemaException e) {
            if (version != fallbackVersion) {
                buffer.position(bufferPosition);
                return responseSchema(fallbackVersion).read(buffer);
            } else
                throw e;
        }
    }

    private Schema schemaFor(Schema[][] schemas, short version) {
        if (id > schemas.length)
            throw new IllegalArgumentException("No schema available for API key " + this);
        if (version < 0 || version > latestVersion())
            throw new IllegalArgumentException("Invalid version for API key " + this + ": " + version);

        Schema[] versions = schemas[id];
        if (versions[version] == null)
            throw new IllegalArgumentException("Unsupported version for API key " + this + ": " + version);
        return versions[version];
    }

    private static String toHtml() {
        final StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>");
        b.append("<th>Name</th>\n");
        b.append("<th>Key</th>\n");
        b.append("</tr>");
        for (ApiKeys key : ApiKeys.values()) {
            b.append("<tr>\n");
            b.append("<td>");
            b.append("<a href=\"#The_Messages_" + key.name + "\">" + key.name + "</a>");
            b.append("</td>");
            b.append("<td>");
            b.append(key.id);
            b.append("</td>");
            b.append("</tr>\n");
        }
        b.append("</table>\n");
        return b.toString();
    }

    public static void main(String[] args) {
        System.out.println(toHtml());
    }

}
