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

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.RecordBatch;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.kafka.common.protocol.types.Type.BYTES;
import static org.apache.kafka.common.protocol.types.Type.COMPACT_BYTES;
import static org.apache.kafka.common.protocol.types.Type.COMPACT_NULLABLE_BYTES;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_BYTES;
import static org.apache.kafka.common.protocol.types.Type.RECORDS;

/**
 * Identifiers for all the Kafka APIs
 */
public enum ApiKeys {
    PRODUCE(ApiMessageType.PRODUCE),
    FETCH(ApiMessageType.FETCH),
    LIST_OFFSETS(ApiMessageType.LIST_OFFSETS),
    METADATA(ApiMessageType.METADATA),
    LEADER_AND_ISR(ApiMessageType.LEADER_AND_ISR, true),
    STOP_REPLICA(ApiMessageType.STOP_REPLICA, true),
    UPDATE_METADATA(ApiMessageType.UPDATE_METADATA, true),
    CONTROLLED_SHUTDOWN(ApiMessageType.CONTROLLED_SHUTDOWN, true),
    OFFSET_COMMIT(ApiMessageType.OFFSET_COMMIT),
    OFFSET_FETCH(ApiMessageType.OFFSET_FETCH),
    FIND_COORDINATOR(ApiMessageType.FIND_COORDINATOR),
    JOIN_GROUP(ApiMessageType.JOIN_GROUP),
    HEARTBEAT(ApiMessageType.HEARTBEAT),
    LEAVE_GROUP(ApiMessageType.LEAVE_GROUP),
    SYNC_GROUP(ApiMessageType.SYNC_GROUP),
    DESCRIBE_GROUPS(ApiMessageType.DESCRIBE_GROUPS),
    LIST_GROUPS(ApiMessageType.LIST_GROUPS),
    SASL_HANDSHAKE(ApiMessageType.SASL_HANDSHAKE),
    API_VERSIONS(ApiMessageType.API_VERSIONS),
    CREATE_TOPICS(ApiMessageType.CREATE_TOPICS, false, true),
    DELETE_TOPICS(ApiMessageType.DELETE_TOPICS, false, true),
    DELETE_RECORDS(ApiMessageType.DELETE_RECORDS),
    INIT_PRODUCER_ID(ApiMessageType.INIT_PRODUCER_ID),
    OFFSET_FOR_LEADER_EPOCH(ApiMessageType.OFFSET_FOR_LEADER_EPOCH),
    ADD_PARTITIONS_TO_TXN(ApiMessageType.ADD_PARTITIONS_TO_TXN, false, RecordBatch.MAGIC_VALUE_V2, false),
    ADD_OFFSETS_TO_TXN(ApiMessageType.ADD_OFFSETS_TO_TXN, false, RecordBatch.MAGIC_VALUE_V2, false),
    END_TXN(ApiMessageType.END_TXN, false, RecordBatch.MAGIC_VALUE_V2, false),
    WRITE_TXN_MARKERS(ApiMessageType.WRITE_TXN_MARKERS, true, RecordBatch.MAGIC_VALUE_V2, false),
    TXN_OFFSET_COMMIT(ApiMessageType.TXN_OFFSET_COMMIT, false, RecordBatch.MAGIC_VALUE_V2, false),
    DESCRIBE_ACLS(ApiMessageType.DESCRIBE_ACLS),
    CREATE_ACLS(ApiMessageType.CREATE_ACLS, false, true),
    DELETE_ACLS(ApiMessageType.DELETE_ACLS, false, true),
    DESCRIBE_CONFIGS(ApiMessageType.DESCRIBE_CONFIGS),
    ALTER_CONFIGS(ApiMessageType.ALTER_CONFIGS, false, true),
    ALTER_REPLICA_LOG_DIRS(ApiMessageType.ALTER_REPLICA_LOG_DIRS),
    DESCRIBE_LOG_DIRS(ApiMessageType.DESCRIBE_LOG_DIRS),
    SASL_AUTHENTICATE(ApiMessageType.SASL_AUTHENTICATE),
    CREATE_PARTITIONS(ApiMessageType.CREATE_PARTITIONS, false, true),
    CREATE_DELEGATION_TOKEN(ApiMessageType.CREATE_DELEGATION_TOKEN, false, true),
    RENEW_DELEGATION_TOKEN(ApiMessageType.RENEW_DELEGATION_TOKEN, false, true),
    EXPIRE_DELEGATION_TOKEN(ApiMessageType.EXPIRE_DELEGATION_TOKEN, false, true),
    DESCRIBE_DELEGATION_TOKEN(ApiMessageType.DESCRIBE_DELEGATION_TOKEN),
    DELETE_GROUPS(ApiMessageType.DELETE_GROUPS),
    ELECT_LEADERS(ApiMessageType.ELECT_LEADERS),
    INCREMENTAL_ALTER_CONFIGS(ApiMessageType.INCREMENTAL_ALTER_CONFIGS, false, true),
    ALTER_PARTITION_REASSIGNMENTS(ApiMessageType.ALTER_PARTITION_REASSIGNMENTS, false, true),
    LIST_PARTITION_REASSIGNMENTS(ApiMessageType.LIST_PARTITION_REASSIGNMENTS),
    OFFSET_DELETE(ApiMessageType.OFFSET_DELETE),
    DESCRIBE_CLIENT_QUOTAS(ApiMessageType.DESCRIBE_CLIENT_QUOTAS),
    ALTER_CLIENT_QUOTAS(ApiMessageType.ALTER_CLIENT_QUOTAS, false, true),
    DESCRIBE_USER_SCRAM_CREDENTIALS(ApiMessageType.DESCRIBE_USER_SCRAM_CREDENTIALS),
    ALTER_USER_SCRAM_CREDENTIALS(ApiMessageType.ALTER_USER_SCRAM_CREDENTIALS, false, true),
    VOTE(ApiMessageType.VOTE, true, RecordBatch.MAGIC_VALUE_V0, false, false),
    BEGIN_QUORUM_EPOCH(ApiMessageType.BEGIN_QUORUM_EPOCH, true, RecordBatch.MAGIC_VALUE_V0, false, false),
    END_QUORUM_EPOCH(ApiMessageType.END_QUORUM_EPOCH, true, RecordBatch.MAGIC_VALUE_V0, false, false),
    DESCRIBE_QUORUM(ApiMessageType.DESCRIBE_QUORUM, true, RecordBatch.MAGIC_VALUE_V0, false, false),
    ALTER_ISR(ApiMessageType.ALTER_ISR, true),
    UPDATE_FEATURES(ApiMessageType.UPDATE_FEATURES, false, true),
    ENVELOPE(ApiMessageType.ENVELOPE, true, RecordBatch.MAGIC_VALUE_V0, false, false);

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

    /** the permanent and immutable id of an API - this can't change ever */
    public final short id;

    /** An english description of the api - used for debugging and metric names, it can potentially be changed via a KIP */
    public final String name;

    /** indicates if this is a ClusterAction request used only by brokers */
    public final boolean clusterAction;

    /** indicates the minimum required inter broker magic required to support the API */
    public final byte minRequiredInterBrokerMagic;

    /** indicates whether the API is enabled and should be exposed in ApiVersions **/
    public final boolean isEnabled;

    /** indicates whether the API is enabled for forwarding **/
    public final boolean forwardable;

    public final boolean requiresDelayedAllocation;

    public final ApiMessageType messageType;

    ApiKeys(ApiMessageType messageType) {
        this(messageType, false);
    }

    ApiKeys(ApiMessageType messageType, boolean clusterAction) {
        this(messageType, clusterAction, RecordBatch.MAGIC_VALUE_V0, false);
    }

    ApiKeys(ApiMessageType messageType, boolean clusterAction, boolean forwardable) {
        this(messageType, clusterAction, RecordBatch.MAGIC_VALUE_V0, forwardable);
    }

    ApiKeys(ApiMessageType messageType, boolean clusterAction, byte minRequiredInterBrokerMagic, boolean forwardable) {
        this(messageType, clusterAction, minRequiredInterBrokerMagic, forwardable, true);
    }

    ApiKeys(
        ApiMessageType messageType,
        boolean clusterAction,
        byte minRequiredInterBrokerMagic,
        boolean forwardable,
        boolean isEnabled
    ) {
        short id = messageType.apiKey();
        if (id < 0)
            throw new IllegalArgumentException("id must not be negative, id: " + id);

        Schema[] requestSchemas = messageType.requestSchemas();
        Schema[] responseSchemas = messageType.responseSchemas();
        String name = messageType.name;
        if (requestSchemas.length != responseSchemas.length)
            throw new IllegalStateException(requestSchemas.length + " request versions for api " + name
                    + " but " + responseSchemas.length + " response versions.");

        for (int i = 0; i < requestSchemas.length; ++i) {
            if (requestSchemas[i] == null)
                throw new IllegalStateException("Request schema for api " + name + " for version " + i + " is null");
            if (responseSchemas[i] == null)
                throw new IllegalStateException("Response schema for api " + name + " for version " + i + " is null");
        }

        this.messageType = messageType;
        this.id = id;
        this.name = name;
        this.clusterAction = clusterAction;
        this.minRequiredInterBrokerMagic = minRequiredInterBrokerMagic;
        this.isEnabled = isEnabled;


        this.requiresDelayedAllocation = forwardable || shouldRetainsBufferReference(requestSchemas);
        this.forwardable = forwardable;
    }

    private static boolean shouldRetainsBufferReference(Schema[] requestSchemas) {
        boolean requestRetainsBufferReference = false;
        for (Schema requestVersionSchema : requestSchemas) {
            if (retainsBufferReference(requestVersionSchema)) {
                requestRetainsBufferReference = true;
                break;
            }
        }
        return requestRetainsBufferReference;
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
        return (short) (messageType.requestSchemas().length - 1);
    }

    public short oldestVersion() {
        return 0;
    }

    public boolean isVersionSupported(short apiVersion) {
        return apiVersion >= oldestVersion() && apiVersion <= latestVersion();
    }

    public short requestHeaderVersion(short apiVersion) {
        return messageType.requestHeaderVersion(apiVersion);
    }

    public short responseHeaderVersion(short apiVersion) {
        return messageType.responseHeaderVersion(apiVersion);
    }

    private static String toHtml() {
        final StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>");
        b.append("<th>Name</th>\n");
        b.append("<th>Key</th>\n");
        b.append("</tr>");
        for (ApiKeys key : ApiKeys.enabledApis()) {
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

    private static boolean retainsBufferReference(Schema schema) {
        final AtomicBoolean hasBuffer = new AtomicBoolean(false);
        Schema.Visitor detector = new Schema.Visitor() {
            @Override
            public void visit(Type field) {
                if (field == BYTES || field == NULLABLE_BYTES || field == RECORDS ||
                    field == COMPACT_BYTES || field == COMPACT_NULLABLE_BYTES)
                    hasBuffer.set(true);
            }
        };
        schema.walk(detector);
        return hasBuffer.get();
    }

    public static List<ApiKeys> enabledApis() {
        return Arrays.stream(values())
            .filter(api -> api.isEnabled)
            .collect(Collectors.toList());
    }

}
