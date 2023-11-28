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
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.RecordBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
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
    ELECT_LEADERS(ApiMessageType.ELECT_LEADERS, false, true),
    INCREMENTAL_ALTER_CONFIGS(ApiMessageType.INCREMENTAL_ALTER_CONFIGS, false, true),
    ALTER_PARTITION_REASSIGNMENTS(ApiMessageType.ALTER_PARTITION_REASSIGNMENTS, false, true),
    LIST_PARTITION_REASSIGNMENTS(ApiMessageType.LIST_PARTITION_REASSIGNMENTS, false, true),
    OFFSET_DELETE(ApiMessageType.OFFSET_DELETE),
    DESCRIBE_CLIENT_QUOTAS(ApiMessageType.DESCRIBE_CLIENT_QUOTAS),
    ALTER_CLIENT_QUOTAS(ApiMessageType.ALTER_CLIENT_QUOTAS, false, true),
    DESCRIBE_USER_SCRAM_CREDENTIALS(ApiMessageType.DESCRIBE_USER_SCRAM_CREDENTIALS),
    ALTER_USER_SCRAM_CREDENTIALS(ApiMessageType.ALTER_USER_SCRAM_CREDENTIALS, false, true),
    VOTE(ApiMessageType.VOTE, true, RecordBatch.MAGIC_VALUE_V0, false),
    BEGIN_QUORUM_EPOCH(ApiMessageType.BEGIN_QUORUM_EPOCH, true, RecordBatch.MAGIC_VALUE_V0, false),
    END_QUORUM_EPOCH(ApiMessageType.END_QUORUM_EPOCH, true, RecordBatch.MAGIC_VALUE_V0, false),
    DESCRIBE_QUORUM(ApiMessageType.DESCRIBE_QUORUM, true, RecordBatch.MAGIC_VALUE_V0, true),
    ALTER_PARTITION(ApiMessageType.ALTER_PARTITION, true),
    UPDATE_FEATURES(ApiMessageType.UPDATE_FEATURES, true, true),
    ENVELOPE(ApiMessageType.ENVELOPE, true, RecordBatch.MAGIC_VALUE_V0, false),
    FETCH_SNAPSHOT(ApiMessageType.FETCH_SNAPSHOT, false, RecordBatch.MAGIC_VALUE_V0, false),
    DESCRIBE_CLUSTER(ApiMessageType.DESCRIBE_CLUSTER),
    DESCRIBE_PRODUCERS(ApiMessageType.DESCRIBE_PRODUCERS),
    BROKER_REGISTRATION(ApiMessageType.BROKER_REGISTRATION, true, RecordBatch.MAGIC_VALUE_V0, false),
    BROKER_HEARTBEAT(ApiMessageType.BROKER_HEARTBEAT, true, RecordBatch.MAGIC_VALUE_V0, false),
    UNREGISTER_BROKER(ApiMessageType.UNREGISTER_BROKER, false, RecordBatch.MAGIC_VALUE_V0, true),
    DESCRIBE_TRANSACTIONS(ApiMessageType.DESCRIBE_TRANSACTIONS),
    LIST_TRANSACTIONS(ApiMessageType.LIST_TRANSACTIONS),
    ALLOCATE_PRODUCER_IDS(ApiMessageType.ALLOCATE_PRODUCER_IDS, true, true),
    CONSUMER_GROUP_HEARTBEAT(ApiMessageType.CONSUMER_GROUP_HEARTBEAT),
    CONSUMER_GROUP_DESCRIBE(ApiMessageType.CONSUMER_GROUP_DESCRIBE),
    CONTROLLER_REGISTRATION(ApiMessageType.CONTROLLER_REGISTRATION),
    GET_TELEMETRY_SUBSCRIPTIONS(ApiMessageType.GET_TELEMETRY_SUBSCRIPTIONS),
    PUSH_TELEMETRY(ApiMessageType.PUSH_TELEMETRY),
    ASSIGN_REPLICAS_TO_DIRS(ApiMessageType.ASSIGN_REPLICAS_TO_DIRS);

    private static final Map<ApiMessageType.ListenerType, EnumSet<ApiKeys>> APIS_BY_LISTENER =
        new EnumMap<>(ApiMessageType.ListenerType.class);

    static {
        for (ApiMessageType.ListenerType listenerType : ApiMessageType.ListenerType.values()) {
            APIS_BY_LISTENER.put(listenerType, filterApisForListener(listenerType));
        }
    }

    // The generator ensures every `ApiMessageType` has a unique id
    private static final Map<Integer, ApiKeys> ID_TO_TYPE = Arrays.stream(ApiKeys.values())
        .collect(Collectors.toMap(key -> (int) key.id, Function.identity()));

    /** the permanent and immutable id of an API - this can't change ever */
    public final short id;

    /** An english description of the api - used for debugging and metric names, it can potentially be changed via a KIP */
    public final String name;

    /** indicates if this is a ClusterAction request used only by brokers */
    public final boolean clusterAction;

    /** indicates the minimum required inter broker magic required to support the API */
    public final byte minRequiredInterBrokerMagic;

    /** indicates whether the API is enabled for forwarding */
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

    ApiKeys(
        ApiMessageType messageType,
        boolean clusterAction,
        byte minRequiredInterBrokerMagic,
        boolean forwardable
    ) {
        this.messageType = messageType;
        this.id = messageType.apiKey();
        this.name = messageType.name;
        this.clusterAction = clusterAction;
        this.minRequiredInterBrokerMagic = minRequiredInterBrokerMagic;
        this.requiresDelayedAllocation = forwardable || shouldRetainsBufferReference(messageType.requestSchemas());
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
        ApiKeys apiKey = ID_TO_TYPE.get(id);
        if (apiKey == null) {
            throw new IllegalArgumentException("Unexpected api key: " + id);
        }
        return apiKey;
    }

    public static boolean hasId(int id) {
        return ID_TO_TYPE.containsKey(id);
    }

    public short latestVersion() {
        return messageType.highestSupportedVersion(true);
    }

    public short latestVersion(boolean enableUnstableLastVersion) {
        return messageType.highestSupportedVersion(enableUnstableLastVersion);
    }

    public short oldestVersion() {
        return messageType.lowestSupportedVersion();
    }

    public List<Short> allVersions() {
        List<Short> versions = new ArrayList<>(latestVersion() - oldestVersion() + 1);
        for (short version = oldestVersion(); version <= latestVersion(); version++) {
            versions.add(version);
        }
        return versions;
    }

    public boolean isVersionSupported(short apiVersion) {
        return apiVersion >= oldestVersion() && apiVersion <= latestVersion();
    }

    public boolean isVersionEnabled(short apiVersion, boolean enableUnstableLastVersion) {
        // ApiVersions API is a particular case. The client always send the highest version
        // that it supports and the server fails back to version 0 if it does not know it.
        // Hence, we have to accept any versions here, even unsupported ones.
        if (this == ApiKeys.API_VERSIONS) return true;

        return apiVersion >= oldestVersion() && apiVersion <= latestVersion(enableUnstableLastVersion);
    }

    public Optional<ApiVersionsResponseData.ApiVersion> toApiVersion(boolean enableUnstableLastVersion) {
        short oldestVersion = oldestVersion();
        short latestVersion = latestVersion(enableUnstableLastVersion);

        // API is entirely disabled if latestStableVersion is smaller than oldestVersion.
        if (latestVersion >= oldestVersion) {
            return Optional.of(new ApiVersionsResponseData.ApiVersion()
               .setApiKey(messageType.apiKey())
               .setMinVersion(oldestVersion)
               .setMaxVersion(latestVersion));
        } else {
            return Optional.empty();
        }
    }

    public short requestHeaderVersion(short apiVersion) {
        return messageType.requestHeaderVersion(apiVersion);
    }

    public short responseHeaderVersion(short apiVersion) {
        return messageType.responseHeaderVersion(apiVersion);
    }

    public boolean inScope(ApiMessageType.ListenerType listener) {
        return messageType.listeners().contains(listener);
    }

    private static String toHtml() {
        final StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>");
        b.append("<th>Name</th>\n");
        b.append("<th>Key</th>\n");
        b.append("</tr>");
        for (ApiKeys key : clientApis()) {
            b.append("<tr>\n");
            b.append("<td>");
            b.append("<a href=\"#The_Messages_" + key.name + "\">" + key.name + "</a>");
            b.append("</td>");
            b.append("<td>");
            b.append(key.id);
            b.append("</td>");
            b.append("</tr>\n");
        }
        b.append("</tbody></table>\n");
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

    public static EnumSet<ApiKeys> zkBrokerApis() {
        return apisForListener(ApiMessageType.ListenerType.ZK_BROKER);
    }

    public static EnumSet<ApiKeys> kraftBrokerApis() {
        return apisForListener(ApiMessageType.ListenerType.BROKER);
    }

    public static EnumSet<ApiKeys> controllerApis() {
        return apisForListener(ApiMessageType.ListenerType.CONTROLLER);
    }

    public static EnumSet<ApiKeys> clientApis() {
        List<ApiKeys> apis = Arrays.stream(ApiKeys.values())
            .filter(apiKey -> apiKey.inScope(ApiMessageType.ListenerType.ZK_BROKER) || apiKey.inScope(ApiMessageType.ListenerType.BROKER))
            .collect(Collectors.toList());
        return EnumSet.copyOf(apis);
    }

    public static EnumSet<ApiKeys> apisForListener(ApiMessageType.ListenerType listener) {
        return APIS_BY_LISTENER.get(listener);
    }

    private static EnumSet<ApiKeys> filterApisForListener(ApiMessageType.ListenerType listener) {
        List<ApiKeys> apis = Arrays.stream(ApiKeys.values())
            .filter(apiKey -> apiKey.inScope(listener))
            .collect(Collectors.toList());
        return EnumSet.copyOf(apis);
    }
}
