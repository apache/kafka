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
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.AlterConfigsResponse;
import org.apache.kafka.common.requests.AlterReplicaLogDirsRequest;
import org.apache.kafka.common.requests.AlterReplicaLogDirsResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.ControlledShutdownRequest;
import org.apache.kafka.common.requests.ControlledShutdownResponse;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreateAclsResponse;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteAclsResponse;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.DescribeLogDirsRequest;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaderAndIsrRequest;
import org.apache.kafka.common.requests.LeaderAndIsrResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.requests.StopReplicaRequest;
import org.apache.kafka.common.requests.StopReplicaResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;
import org.apache.kafka.common.requests.UpdateMetadataRequest;
import org.apache.kafka.common.requests.UpdateMetadataResponse;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.protocol.types.Type.BYTES;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_BYTES;
import static org.apache.kafka.common.protocol.types.Type.RECORDS;

/**
 * Identifiers for all the Kafka APIs
 */
public enum ApiKeys {
    PRODUCE(0, "Produce",
            "Sent by a producer client to a leader broker to " +
            "request that the data be appended to the given partition's log.",
            ProduceRequest.schemaVersions(), ProduceResponse.schemaVersions()),
    FETCH(1, "Fetch",
            "Sent by a consumer (which could be a following broker) to a leader broker to fetch records for the " +
            "partitions present in the request.",
            FetchRequest.schemaVersions(), FetchResponse.schemaVersions()),
    LIST_OFFSETS(2, "ListOffsets",
            "Sent by a consumer (which could be a following broker) to a leader broker to obtain the consumer offsets " +
            "for the requested partitions.",
            ListOffsetRequest.schemaVersions(), ListOffsetResponse.schemaVersions()),
    METADATA(3, "Metadata",
            "Sent by a client to a broker to obtain metadata about the cluster and the topics in it. " +
            "A metadata request is required to bootstrap a client, so it knows about the cluster controller " +
            "and partitions leaders.",
            MetadataRequest.schemaVersions(), MetadataResponse.schemaVersions()),
    LEADER_AND_ISR(4, "LeaderAndIsr", true,
            "Sent by the controller to other brokers to change the whether they are leaders or followers for " +
            "the partitions given in the request.",
            LeaderAndIsrRequest.schemaVersions(), LeaderAndIsrResponse.schemaVersions()),
    STOP_REPLICA(5, "StopReplica", true,
            "Sent by the controller to brokers hosting replicas of partitions when the partitions are going " +
            "offline or should be deleted.",
            StopReplicaRequest.schemaVersions(), StopReplicaResponse.schemaVersions()),
    UPDATE_METADATA(6, "UpdateMetadata", true,
            "Sent by the controller to other brokers in the cluster to get them to update their metadata about " +
            "the cluster.",
            UpdateMetadataRequest.schemaVersions(), UpdateMetadataResponse.schemaVersions()),
    CONTROLLED_SHUTDOWN(7, "ControlledShutdown", true,
            "Sent by a broker that is about to shutdown to the controller, so that the controller " +
            "can arrange for the partitions currently lead by that broker to be led by other brokers.",
            ControlledShutdownRequest.schemaVersions(), ControlledShutdownResponse.schemaVersions()),
    OFFSET_COMMIT(8, "OffsetCommit",
            "Sent by a consumer to the consumer group coordinator to save the consumer's offset.",
            OffsetCommitRequest.schemaVersions(), OffsetCommitResponse.schemaVersions()),
    OFFSET_FETCH(9, "OffsetFetch",
            "Sent by a consumer to the consumer group coordinator to recover the consumer's offset.",
            OffsetFetchRequest.schemaVersions(), OffsetFetchResponse.schemaVersions()),
    FIND_COORDINATOR(10, "FindCoordinator",
            "Sent by client to any broker to find the broker acting as the coordinator for the given group or transaction. " +
            "Renamed and generalised by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging'>KIP-98</a>.",
            FindCoordinatorRequest.schemaVersions(), FindCoordinatorResponse.schemaVersions()),
    JOIN_GROUP(11, "JoinGroup",
            "Sent by a consumer to the broker acting as consumer group coordinator " +
            "to join the given consumer group. " +
            "The coordinator will elect one client the leader. " +
            "Only if the client is elected leader will the response include group membership. " +
            "Specified in the <a href='https://cwiki.apache.org/confluence/display/KAFKA/Kafka+0.9+Consumer+Rewrite+Design'>Kafka 0.9 Consumer Rewrite Design</a>",
            JoinGroupRequest.schemaVersions(), JoinGroupResponse.schemaVersions()),
    HEARTBEAT(12, "Heartbeat",
            "Sent by a consumer to the broker acting as consumer group coordinator " +
            "to verify that the assigned partitions from a " +
            "previous SYNC_GROUP request are still valid. " +
            "Specified in the <a href='https://cwiki.apache.org/confluence/display/KAFKA/Kafka+0.9+Consumer+Rewrite+Design'>Kafka 0.9 Consumer Rewrite Design</a>",
            HeartbeatRequest.schemaVersions(), HeartbeatResponse.schemaVersions()),
    LEAVE_GROUP(13, "LeaveGroup",
            "Sent by a consumer to the consumer group coordinator when the consumer is leaving the group, " +
            "triggering a rebalance via the HEARTBEAT/JOIN_GROUP/SYNC_GROUP protocol.",
            LeaveGroupRequest.schemaVersions(), LeaveGroupResponse.schemaVersions()),
    SYNC_GROUP(14, "SyncGroup",
            "Sent by a consumer the the broker acting as consumer group coordinator " +
            "following a JOIN_GROUP exchange. The request from the " +
            "consumer that was elected as group leader provides the group state " +
            "(in particular the assignment of consumers to partitions) and " +
            "this is forwarded as the response to the remaining group members." +
            "Specified in the <a href='https://cwiki.apache.org/confluence/display/KAFKA/Kafka+0.9+Consumer+Rewrite+Design'>Kafka 0.9 Consumer Rewrite Design</a>",
            SyncGroupRequest.schemaVersions(), SyncGroupResponse.schemaVersions()),
    DESCRIBE_GROUPS(15, "DescribeGroups",
            "Sent by an (admin) client to a broker to get detailed information (such as group membersip) about a specific consumer group. " +
            "Specified by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-40%3A+ListGroups+and+DescribeGroup'>KIP-40</a>.",
            DescribeGroupsRequest.schemaVersions(), DescribeGroupsResponse.schemaVersions()),
    LIST_GROUPS(16, "ListGroups",
            "Sent by an (admin) client to a particular broker to get information about the consumers groups managed by that broker. " +
            "To get a list of all consumers groups in the cluster, it needs to be sent to all brokers." +
            "Specified by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-40%3A+ListGroups+and+DescribeGroup'>KIP-40</a>.",
            ListGroupsRequest.schemaVersions(), ListGroupsResponse.schemaVersions()),
    SASL_HANDSHAKE(17, "SaslHandshake",
            "Sent by a client to a broker to supply the SASL mechanism. Following the receipt of a successful " +
            "handshake response the client performs SASL authentication using the accepted mechanism outside " +
            "the Kafka protcol. " +
            "Specified in <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-43%3A+Kafka+SASL+enhancements'>KIP-43</a>.",
            SaslHandshakeRequest.schemaVersions(), SaslHandshakeResponse.schemaVersions()),
    API_VERSIONS(18, "ApiVersions",
            "Sent by a client to a broker to discover the protocol versions the broker supports. " +
            "The response enumerates each supported ApiKey together with the minimum and maximum supported " +
            "versions of that key. " +
            "Specified in <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-35+-+Retrieving+protocol+version'>KIP-35</a>.",
            ApiVersionsRequest.schemaVersions(), ApiVersionsResponse.schemaVersions()) {
        @Override
        public Struct parseResponse(short version, ByteBuffer buffer) {
            // Fallback to version 0 for ApiVersions response. If a client sends an ApiVersionsRequest
            // using a version higher than that supported by the broker, a version 0 response is sent
            // to the client indicating UNSUPPORTED_VERSION.
            return parseResponse(version, buffer, (short) 0);
        }
    },
    CREATE_TOPICS(19, "CreateTopics",
            "Sent by an (admin) client to the controller to create the topics given in the request.",
            CreateTopicsRequest.schemaVersions(), CreateTopicsResponse.schemaVersions()),
    DELETE_TOPICS(20, "DeleteTopics",
            "Sent by an (admin) client to the controller to delete the topics given in the request.",
            DeleteTopicsRequest.schemaVersions(), DeleteTopicsResponse.schemaVersions()),
    DELETE_RECORDS(21, "DeleteRecords",
            "Sent by an client to the partition leader to request the deletion of records from topic partitions, " +
            "as identified in the request.",
            DeleteRecordsRequest.schemaVersions(), DeleteRecordsResponse.schemaVersions()),
    INIT_PRODUCER_ID(22, "InitProducerId",
            "Sent by a producer to its transaction coordinator to to get the assigned PID, increment its epoch, and " +
            "fence any previous producers sharing the same TransactionalId. " +
            "Specified by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging'>KIP-98</a>.",
            InitProducerIdRequest.schemaVersions(), InitProducerIdResponse.schemaVersions()),
    OFFSET_FOR_LEADER_EPOCH(23, "OffsetForLeaderEpoch", true,
            "Sent by a follower to the leader to retrieve the start offset corresponding to the given leader epochs. " +
            "The follower uses this offset to truncate its log. " +
            "Specified in <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation'>KIP-101</a>.",
            OffsetsForLeaderEpochRequest.schemaVersions(), OffsetsForLeaderEpochResponse.schemaVersions()),
    ADD_PARTITIONS_TO_TXN(24, "AddPartitionsToTxn", false, RecordBatch.MAGIC_VALUE_V2,
            "Sent by a producer to its transaction coordinator to add a partition to the current ongoing transaction. " +
            "Specified by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging'>KIP-98</a>.",
            AddPartitionsToTxnRequest.schemaVersions(), AddPartitionsToTxnResponse.schemaVersions()),
    ADD_OFFSETS_TO_TXN(25, "AddOffsetsToTxn", false, RecordBatch.MAGIC_VALUE_V2,
            "Sent by the producer to its transaction coordinator to indicate a consumer offset commit operation is " +
            "called as part of the current ongoing transaction. " +
            "Specified by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging'>KIP-98</a>.",
            AddOffsetsToTxnRequest.schemaVersions(), AddOffsetsToTxnResponse.schemaVersions()),
    END_TXN(26, "EndTxn", false, RecordBatch.MAGIC_VALUE_V2,
            "Sent by producer to its transaction coordinator to prepare committing or aborting the current " +
            "ongoing transaction. " +
            "Specified by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging'>KIP-98</a>.",
            EndTxnRequest.schemaVersions(), EndTxnResponse.schemaVersions()),
    WRITE_TXN_MARKERS(27, "WriteTxnMarkers", true, RecordBatch.MAGIC_VALUE_V2,
            "Sent by transaction coordinator to broker to commit the transaction. " +
            "Specified by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging'>KIP-98</a>.",
            WriteTxnMarkersRequest.schemaVersions(), WriteTxnMarkersResponse.schemaVersions()),
    TXN_OFFSET_COMMIT(28, "TxnOffsetCommit", false, RecordBatch.MAGIC_VALUE_V2,
            "Sent by transactional producers to consumer group coordinator to commit offsets within a single " +
            "transaction. " +
            "Specified by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging'>KIP-98</a>.",
            TxnOffsetCommitRequest.schemaVersions(), TxnOffsetCommitResponse.schemaVersions()),
    DESCRIBE_ACLS(29, "DescribeAcls",
            "Sent by an (admin) client to any broker to request the details of access control lists. " +
            "Specified by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-140%3A+Add+administrative+RPCs+for+adding%2C+deleting%2C+and+listing+ACLs'>KIP-140</a>.",
            DescribeAclsRequest.schemaVersions(), DescribeAclsResponse.schemaVersions()),
    CREATE_ACLS(30, "CreateAcls",
            "Sent by an (admin) client to any broker to create access control lists for " +
            "managing access to the given entities. " +
            "Specified by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-140%3A+Add+administrative+RPCs+for+adding%2C+deleting%2C+and+listing+ACLs'>KIP-140</a>.",
            CreateAclsRequest.schemaVersions(), CreateAclsResponse.schemaVersions()),
    DELETE_ACLS(31, "DeleteAcls",
            "Sent by an (admin) client to any broker to request the deletion of the given access control lists. " +
            "Specified by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-140%3A+Add+administrative+RPCs+for+adding%2C+deleting%2C+and+listing+ACLs'>KIP-140</a>.",
            DeleteAclsRequest.schemaVersions(), DeleteAclsResponse.schemaVersions()),
    DESCRIBE_CONFIGS(32, "DescribeConfigs",
            "Sent by an (admin) client to a broker to obtain the configs of the given entites. " +
            "For describing broker configs the request must be sent to the required broker. " +
            "For describing topic configs the request can be send to any broker. " +
            "Specified by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-133%3A+Describe+and+Alter+Configs+Admin+APIs'>KIP-133</a>.",
            DescribeConfigsRequest.schemaVersions(), DescribeConfigsResponse.schemaVersions()),
    ALTER_CONFIGS(33, "AlterConfigs",
            "Sent by an (admin) client to any broker to change the configs of the entities given in the request." +
            "Currently only topic config alteration is supported. " +
            "Specified by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-133%3A+Describe+and+Alter+Configs+Admin+APIs'>KIP-133</a>.",
            AlterConfigsRequest.schemaVersions(), AlterConfigsResponse.schemaVersions()),
    ALTER_REPLICA_LOG_DIRS(34, "AlterReplicaLogDirs",
            "Sent by an (admin) client to a particular broker to have it change the log directory it uses to " +
            "store the logs of the given partitions." +
            "Specified by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-113%3A+Support+replicas+movement+between+log+directories'>KIP-113</a>.",
            AlterReplicaLogDirsRequest.schemaVersions(), AlterReplicaLogDirsResponse.schemaVersions()),
    DESCRIBE_LOG_DIRS(35, "DescribeLogDirs",
            "Sent by an (admin) client to a particular broker to discover the log dirs available on that broker." +
            "Specified by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-113%3A+Support+replicas+movement+between+log+directories'>KIP-113</a>.",
            DescribeLogDirsRequest.schemaVersions(), DescribeLogDirsResponse.schemaVersions()),
    SASL_AUTHENTICATE(36, "SaslAuthenticate",
            "Sent by a client to a broker wrapping a SASL authentication message so that the Kafka response can " +
            "include a mechanism-independent indication of authentication fail that is distinguishable from " +
            "a broker failure by the client." +
            "Specified by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-152+-+Improve+diagnostics+for+SASL+authentication+failures'>KIP-152</a>.",
            SaslAuthenticateRequest.schemaVersions(), SaslAuthenticateResponse.schemaVersions()),
    CREATE_PARTITIONS(37, "CreatePartitions",
            "Sent by an (admin) client to the controller to request partitions be added to a topic." +
            "Specified by <a href='https://cwiki.apache.org/confluence/display/KAFKA/KIP-195%3A+AdminClient.createPartitions'>KIP-195</a>",
            CreatePartitionsRequest.schemaVersions(), CreatePartitionsResponse.schemaVersions());

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

    public final String doc;
    public final Schema[] requestSchemas;
    public final Schema[] responseSchemas;
    public final boolean requiresDelayedAllocation;

    ApiKeys(int id, String name, String doc, Schema[] requestSchemas, Schema[] responseSchemas) {
        this(id, name, false, doc, requestSchemas, responseSchemas);
    }

    ApiKeys(int id, String name, boolean clusterAction, String doc, Schema[] requestSchemas, Schema[] responseSchemas) {
        this(id, name, clusterAction, RecordBatch.MAGIC_VALUE_V0, doc, requestSchemas, responseSchemas);
    }

    ApiKeys(int id, String name, boolean clusterAction, byte minRequiredInterBrokerMagic,
            String doc, Schema[] requestSchemas, Schema[] responseSchemas) {
        if (id < 0)
            throw new IllegalArgumentException("id must not be negative, id: " + id);
        this.id = (short) id;
        this.name = name;
        this.clusterAction = clusterAction;
        this.minRequiredInterBrokerMagic = minRequiredInterBrokerMagic;

        if (requestSchemas.length != responseSchemas.length)
            throw new IllegalStateException(requestSchemas.length + " request versions for api " + name
                    + " but " + responseSchemas.length + " response versions.");

        for (int i = 0; i < requestSchemas.length; ++i) {
            if (requestSchemas[i] == null)
                throw new IllegalStateException("Request schema for api " + name + " for version " + i + " is null");
            if (responseSchemas[i] == null)
                throw new IllegalStateException("Response schema for api " + name + " for version " + i + " is null");
        }

        boolean requestRetainsBufferReference = false;
        for (Schema requestVersionSchema : requestSchemas) {
            if (retainsBufferReference(requestVersionSchema)) {
                requestRetainsBufferReference = true;
                break;
            }
        }
        this.requiresDelayedAllocation = requestRetainsBufferReference;
        this.doc = doc;
        this.requestSchemas = requestSchemas;
        this.responseSchemas = responseSchemas;
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

    private static boolean retainsBufferReference(Schema schema) {
        final AtomicBoolean hasBuffer = new AtomicBoolean(false);
        Schema.Visitor detector = new Schema.Visitor() {
            @Override
            public void visit(Type field) {
                if (field == BYTES || field == NULLABLE_BYTES || field == RECORDS)
                    hasBuffer.set(true);
            }
        };
        schema.walk(detector);
        return hasBuffer.get();
    }

}
