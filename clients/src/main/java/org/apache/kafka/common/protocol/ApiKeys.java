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

import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.AlterClientQuotasRequestData;
import org.apache.kafka.common.message.AlterClientQuotasResponseData;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.AlterIsrRequestData;
import org.apache.kafka.common.message.AlterIsrResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.ControlledShutdownRequestData;
import org.apache.kafka.common.message.ControlledShutdownResponseData;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData;
import org.apache.kafka.common.message.DescribeClientQuotasResponseData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.DescribeDelegationTokenRequestData;
import org.apache.kafka.common.message.DescribeDelegationTokenResponseData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.EnvelopeRequestData;
import org.apache.kafka.common.message.EnvelopeResponseData;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.ExpireDelegationTokenResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.message.LeaderAndIsrResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListOffsetRequestData;
import org.apache.kafka.common.message.ListOffsetResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.message.RenewDelegationTokenResponseData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.message.StopReplicaRequestData;
import org.apache.kafka.common.message.StopReplicaResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.message.UpdateFeaturesResponseData;
import org.apache.kafka.common.message.UpdateMetadataRequestData;
import org.apache.kafka.common.message.UpdateMetadataResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;

import java.nio.ByteBuffer;
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
    PRODUCE(0, "Produce", ProduceRequest.schemaVersions(), ProduceResponse.schemaVersions()),
    FETCH(1, "Fetch", FetchRequestData.SCHEMAS, FetchResponseData.SCHEMAS),
    LIST_OFFSETS(2, "ListOffsets", ListOffsetRequestData.SCHEMAS, ListOffsetResponseData.SCHEMAS),
    METADATA(3, "Metadata", MetadataRequestData.SCHEMAS, MetadataResponseData.SCHEMAS),
    LEADER_AND_ISR(4, "LeaderAndIsr", true, LeaderAndIsrRequestData.SCHEMAS, LeaderAndIsrResponseData.SCHEMAS),
    STOP_REPLICA(5, "StopReplica", true, StopReplicaRequestData.SCHEMAS, StopReplicaResponseData.SCHEMAS),
    UPDATE_METADATA(6, "UpdateMetadata", true, UpdateMetadataRequestData.SCHEMAS, UpdateMetadataResponseData.SCHEMAS),
    CONTROLLED_SHUTDOWN(7, "ControlledShutdown", true, ControlledShutdownRequestData.SCHEMAS,
            ControlledShutdownResponseData.SCHEMAS),
    OFFSET_COMMIT(8, "OffsetCommit", OffsetCommitRequestData.SCHEMAS, OffsetCommitResponseData.SCHEMAS),
    OFFSET_FETCH(9, "OffsetFetch", OffsetFetchRequestData.SCHEMAS, OffsetFetchResponseData.SCHEMAS),
    FIND_COORDINATOR(10, "FindCoordinator", FindCoordinatorRequestData.SCHEMAS,
            FindCoordinatorResponseData.SCHEMAS),
    JOIN_GROUP(11, "JoinGroup", JoinGroupRequestData.SCHEMAS, JoinGroupResponseData.SCHEMAS),
    HEARTBEAT(12, "Heartbeat", HeartbeatRequestData.SCHEMAS, HeartbeatResponseData.SCHEMAS),
    LEAVE_GROUP(13, "LeaveGroup", LeaveGroupRequestData.SCHEMAS, LeaveGroupResponseData.SCHEMAS),
    SYNC_GROUP(14, "SyncGroup", SyncGroupRequestData.SCHEMAS, SyncGroupResponseData.SCHEMAS),
    DESCRIBE_GROUPS(15, "DescribeGroups", DescribeGroupsRequestData.SCHEMAS,
            DescribeGroupsResponseData.SCHEMAS),
    LIST_GROUPS(16, "ListGroups", ListGroupsRequestData.SCHEMAS, ListGroupsResponseData.SCHEMAS),
    SASL_HANDSHAKE(17, "SaslHandshake", SaslHandshakeRequestData.SCHEMAS, SaslHandshakeResponseData.SCHEMAS),
    API_VERSIONS(18, "ApiVersions", ApiVersionsRequestData.SCHEMAS, ApiVersionsResponseData.SCHEMAS) {
        @Override
        public Struct parseResponse(short version, ByteBuffer buffer) {
            // Fallback to version 0 for ApiVersions response. If a client sends an ApiVersionsRequest
            // using a version higher than that supported by the broker, a version 0 response is sent
            // to the client indicating UNSUPPORTED_VERSION.
            return parseResponse(version, buffer, (short) 0);
        }
    },
    CREATE_TOPICS(19, "CreateTopics", CreateTopicsRequestData.SCHEMAS, CreateTopicsResponseData.SCHEMAS, true),
    DELETE_TOPICS(20, "DeleteTopics", DeleteTopicsRequestData.SCHEMAS, DeleteTopicsResponseData.SCHEMAS),
    DELETE_RECORDS(21, "DeleteRecords", DeleteRecordsRequestData.SCHEMAS, DeleteRecordsResponseData.SCHEMAS),
    INIT_PRODUCER_ID(22, "InitProducerId", InitProducerIdRequestData.SCHEMAS, InitProducerIdResponseData.SCHEMAS),
    OFFSET_FOR_LEADER_EPOCH(23, "OffsetForLeaderEpoch", false, OffsetsForLeaderEpochRequest.schemaVersions(),
            OffsetsForLeaderEpochResponse.schemaVersions()),
    ADD_PARTITIONS_TO_TXN(24, "AddPartitionsToTxn", false, RecordBatch.MAGIC_VALUE_V2,
            AddPartitionsToTxnRequestData.SCHEMAS, AddPartitionsToTxnResponseData.SCHEMAS),
    ADD_OFFSETS_TO_TXN(25, "AddOffsetsToTxn", false, RecordBatch.MAGIC_VALUE_V2, AddOffsetsToTxnRequestData.SCHEMAS,
            AddOffsetsToTxnResponseData.SCHEMAS),
    END_TXN(26, "EndTxn", false, RecordBatch.MAGIC_VALUE_V2, EndTxnRequestData.SCHEMAS, EndTxnResponseData.SCHEMAS),
    WRITE_TXN_MARKERS(27, "WriteTxnMarkers", true, RecordBatch.MAGIC_VALUE_V2, WriteTxnMarkersRequestData.SCHEMAS,
            WriteTxnMarkersResponseData.SCHEMAS),
    TXN_OFFSET_COMMIT(28, "TxnOffsetCommit", false, RecordBatch.MAGIC_VALUE_V2, TxnOffsetCommitRequestData.SCHEMAS,
            TxnOffsetCommitResponseData.SCHEMAS),
    DESCRIBE_ACLS(29, "DescribeAcls", DescribeAclsRequestData.SCHEMAS, DescribeAclsResponseData.SCHEMAS),
    CREATE_ACLS(30, "CreateAcls", CreateAclsRequestData.SCHEMAS, CreateAclsResponseData.SCHEMAS),
    DELETE_ACLS(31, "DeleteAcls", DeleteAclsRequestData.SCHEMAS, DeleteAclsResponseData.SCHEMAS),
    DESCRIBE_CONFIGS(32, "DescribeConfigs", DescribeConfigsRequestData.SCHEMAS,
             DescribeConfigsResponseData.SCHEMAS),
    ALTER_CONFIGS(33, "AlterConfigs", AlterConfigsRequestData.SCHEMAS,
            AlterConfigsResponseData.SCHEMAS, true),
    ALTER_REPLICA_LOG_DIRS(34, "AlterReplicaLogDirs", AlterReplicaLogDirsRequestData.SCHEMAS,
            AlterReplicaLogDirsResponseData.SCHEMAS),
    DESCRIBE_LOG_DIRS(35, "DescribeLogDirs", DescribeLogDirsRequestData.SCHEMAS,
            DescribeLogDirsResponseData.SCHEMAS),
    SASL_AUTHENTICATE(36, "SaslAuthenticate", SaslAuthenticateRequestData.SCHEMAS,
            SaslAuthenticateResponseData.SCHEMAS),
    CREATE_PARTITIONS(37, "CreatePartitions", CreatePartitionsRequestData.SCHEMAS,
            CreatePartitionsResponseData.SCHEMAS),
    CREATE_DELEGATION_TOKEN(38, "CreateDelegationToken", CreateDelegationTokenRequestData.SCHEMAS,
            CreateDelegationTokenResponseData.SCHEMAS),
    RENEW_DELEGATION_TOKEN(39, "RenewDelegationToken", RenewDelegationTokenRequestData.SCHEMAS,
            RenewDelegationTokenResponseData.SCHEMAS),
    EXPIRE_DELEGATION_TOKEN(40, "ExpireDelegationToken", ExpireDelegationTokenRequestData.SCHEMAS,
            ExpireDelegationTokenResponseData.SCHEMAS),
    DESCRIBE_DELEGATION_TOKEN(41, "DescribeDelegationToken", DescribeDelegationTokenRequestData.SCHEMAS,
            DescribeDelegationTokenResponseData.SCHEMAS),
    DELETE_GROUPS(42, "DeleteGroups", DeleteGroupsRequestData.SCHEMAS, DeleteGroupsResponseData.SCHEMAS),
    ELECT_LEADERS(43, "ElectLeaders", ElectLeadersRequestData.SCHEMAS,
            ElectLeadersResponseData.SCHEMAS),
    INCREMENTAL_ALTER_CONFIGS(44, "IncrementalAlterConfigs", IncrementalAlterConfigsRequestData.SCHEMAS,
            IncrementalAlterConfigsResponseData.SCHEMAS, true),
    ALTER_PARTITION_REASSIGNMENTS(45, "AlterPartitionReassignments", AlterPartitionReassignmentsRequestData.SCHEMAS,
            AlterPartitionReassignmentsResponseData.SCHEMAS),
    LIST_PARTITION_REASSIGNMENTS(46, "ListPartitionReassignments", ListPartitionReassignmentsRequestData.SCHEMAS,
            ListPartitionReassignmentsResponseData.SCHEMAS),
    OFFSET_DELETE(47, "OffsetDelete", OffsetDeleteRequestData.SCHEMAS, OffsetDeleteResponseData.SCHEMAS),
    DESCRIBE_CLIENT_QUOTAS(48, "DescribeClientQuotas", DescribeClientQuotasRequestData.SCHEMAS,
            DescribeClientQuotasResponseData.SCHEMAS),
    ALTER_CLIENT_QUOTAS(49, "AlterClientQuotas", AlterClientQuotasRequestData.SCHEMAS,
            AlterClientQuotasResponseData.SCHEMAS, true),
    DESCRIBE_USER_SCRAM_CREDENTIALS(50, "DescribeUserScramCredentials", DescribeUserScramCredentialsRequestData.SCHEMAS,
            DescribeUserScramCredentialsResponseData.SCHEMAS),
    ALTER_USER_SCRAM_CREDENTIALS(51, "AlterUserScramCredentials", AlterUserScramCredentialsRequestData.SCHEMAS,
            AlterUserScramCredentialsResponseData.SCHEMAS),
    VOTE(52, "Vote", true, false,
        VoteRequestData.SCHEMAS, VoteResponseData.SCHEMAS),
    BEGIN_QUORUM_EPOCH(53, "BeginQuorumEpoch", true, false,
        BeginQuorumEpochRequestData.SCHEMAS, BeginQuorumEpochResponseData.SCHEMAS),
    END_QUORUM_EPOCH(54, "EndQuorumEpoch", true, false,
        EndQuorumEpochRequestData.SCHEMAS, EndQuorumEpochResponseData.SCHEMAS),
    DESCRIBE_QUORUM(55, "DescribeQuorum", true, false,
        DescribeQuorumRequestData.SCHEMAS, DescribeQuorumResponseData.SCHEMAS),
    ALTER_ISR(56, "AlterIsr", AlterIsrRequestData.SCHEMAS, AlterIsrResponseData.SCHEMAS),
    UPDATE_FEATURES(57, "UpdateFeatures",
        UpdateFeaturesRequestData.SCHEMAS, UpdateFeaturesResponseData.SCHEMAS),
    ENVELOPE(58, "Envelope", true, false, EnvelopeRequestData.SCHEMAS, EnvelopeResponseData.SCHEMAS);

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

    /** indicates the minimum required inter broker magic required to support the API */
    public final byte minRequiredInterBrokerMagic;

    /** indicates whether the API is enabled and should be exposed in ApiVersions **/
    public final boolean isEnabled;

    /** indicates whether the API is enabled for forwarding **/
    public final boolean forwardable;

    public final Schema[] requestSchemas;
    public final Schema[] responseSchemas;
    public final boolean requiresDelayedAllocation;

    ApiKeys(int id, String name, Schema[] requestSchemas, Schema[] responseSchemas) {
        this(id, name, false, requestSchemas, responseSchemas);
    }

    ApiKeys(int id, String name, boolean clusterAction, Schema[] requestSchemas, Schema[] responseSchemas) {
        this(id, name, clusterAction, RecordBatch.MAGIC_VALUE_V0, requestSchemas, responseSchemas);
    }

    ApiKeys(int id, String name, Schema[] requestSchemas, Schema[] responseSchemas, boolean forwardable) {
        this(id, name, false, RecordBatch.MAGIC_VALUE_V0, true, requestSchemas, responseSchemas, forwardable);
    }

    ApiKeys(int id, String name, boolean clusterAction, boolean isEnabled, Schema[] requestSchemas, Schema[] responseSchemas) {
        this(id, name, clusterAction, RecordBatch.MAGIC_VALUE_V0, isEnabled, requestSchemas, responseSchemas, false);
    }

    ApiKeys(int id, String name, boolean clusterAction, byte minRequiredInterBrokerMagic,
            Schema[] requestSchemas, Schema[] responseSchemas) {
        this(id, name, clusterAction, minRequiredInterBrokerMagic, true, requestSchemas, responseSchemas, false);
    }

    ApiKeys(
        int id,
        String name,
        boolean clusterAction,
        byte minRequiredInterBrokerMagic,
        boolean isEnabled,
        Schema[] requestSchemas,
        Schema[] responseSchemas,
        boolean forwardable
    ) {
        if (id < 0)
            throw new IllegalArgumentException("id must not be negative, id: " + id);
        this.id = (short) id;
        this.name = name;
        this.clusterAction = clusterAction;
        this.minRequiredInterBrokerMagic = minRequiredInterBrokerMagic;
        this.isEnabled = isEnabled;

        if (requestSchemas.length != responseSchemas.length)
            throw new IllegalStateException(requestSchemas.length + " request versions for api " + name
                    + " but " + responseSchemas.length + " response versions.");

        for (int i = 0; i < requestSchemas.length; ++i) {
            if (requestSchemas[i] == null)
                throw new IllegalStateException("Request schema for api " + name + " for version " + i + " is null");
            if (responseSchemas[i] == null)
                throw new IllegalStateException("Response schema for api " + name + " for version " + i + " is null");
        }

        this.requiresDelayedAllocation = forwardable || shouldRetainsBufferReference(requestSchemas);
        this.requestSchemas = requestSchemas;
        this.responseSchemas = responseSchemas;
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
        return (short) (requestSchemas.length - 1);
    }

    public short oldestVersion() {
        return 0;
    }

    public Schema requestSchema(short version) {
        return schemaFor(requestSchemas, version);
    }

    public Schema responseSchema(short version) {
        return schemaFor(responseSchemas, version);
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

    private Schema schemaFor(Schema[] versions, short version) {
        if (!isVersionSupported(version))
            throw new IllegalArgumentException("Invalid version for API key " + this + ": " + version);
        return versions[version];
    }

    public boolean isVersionSupported(short apiVersion) {
        return apiVersion >= oldestVersion() && apiVersion <= latestVersion();
    }

    public short requestHeaderVersion(short apiVersion) {
        return ApiMessageType.fromApiKey(id).requestHeaderVersion(apiVersion);
    }

    public short responseHeaderVersion(short apiVersion) {
        return ApiMessageType.fromApiKey(id).responseHeaderVersion(apiVersion);
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
