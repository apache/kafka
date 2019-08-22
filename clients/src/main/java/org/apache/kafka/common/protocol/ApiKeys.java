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
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteAclsResponse;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeLogDirsRequest;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.LeaderAndIsrRequest;
import org.apache.kafka.common.requests.LeaderAndIsrResponse;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.StopReplicaRequest;
import org.apache.kafka.common.requests.StopReplicaResponse;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;
import org.apache.kafka.common.requests.UpdateMetadataRequest;
import org.apache.kafka.common.requests.UpdateMetadataResponse;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.protocol.types.Type.BYTES;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_BYTES;
import static org.apache.kafka.common.protocol.types.Type.RECORDS;

/**
 * Identifiers for all the Kafka APIs
 */
public enum ApiKeys {
    PRODUCE(ProduceRequest.schemaVersions(),
        ProduceResponse.schemaVersions()),
    FETCH(FetchRequest.schemaVersions(),
        FetchResponse.schemaVersions()),
    LIST_OFFSET(ListOffsetRequest.schemaVersions(),
        ListOffsetResponse.schemaVersions()),
    METADATA(),
    LEADER_AND_ISR(LeaderAndIsrRequest.schemaVersions(),
        LeaderAndIsrResponse.schemaVersions(), true),
    STOP_REPLICA(StopReplicaRequest.schemaVersions(),
        StopReplicaResponse.schemaVersions(), true),
    UPDATE_METADATA(UpdateMetadataRequest.schemaVersions(),
        UpdateMetadataResponse.schemaVersions(), true),
    CONTROLLED_SHUTDOWN(true),
    OFFSET_COMMIT(),
    OFFSET_FETCH(),
    FIND_COORDINATOR(),
    JOIN_GROUP(),
    HEARTBEAT(),
    LEAVE_GROUP(),
    SYNC_GROUP(),
    DESCRIBE_GROUPS(),
    LIST_GROUPS(),
    SASL_HANDSHAKE(),
    API_VERSIONS(ApiVersionsRequest.schemaVersions(),
        ApiVersionsResponse.schemaVersions()),
    CREATE_TOPICS(),
    DELETE_TOPICS(),
    DELETE_RECORDS(DeleteRecordsRequest.schemaVersions(),
        DeleteRecordsResponse.schemaVersions()),
    INIT_PRODUCER_ID(),
    OFFSET_FOR_LEADER_EPOCH(OffsetsForLeaderEpochRequest.schemaVersions(),
        OffsetsForLeaderEpochResponse.schemaVersions()),
    ADD_PARTITIONS_TO_TXN(AddPartitionsToTxnRequest.schemaVersions(),
        AddPartitionsToTxnResponse.schemaVersions(),
        RecordBatch.MAGIC_VALUE_V2),
    ADD_OFFSETS_TO_TXN(AddOffsetsToTxnRequest.schemaVersions(),
        AddOffsetsToTxnResponse.schemaVersions(),
        RecordBatch.MAGIC_VALUE_V2),
    END_TXN(EndTxnRequest.schemaVersions(),
        EndTxnResponse.schemaVersions(),
        RecordBatch.MAGIC_VALUE_V2),
    WRITE_TXN_MARKERS(WriteTxnMarkersRequest.schemaVersions(),
        WriteTxnMarkersResponse.schemaVersions(),
        RecordBatch.MAGIC_VALUE_V2,
        true),
    TXN_OFFSET_COMMIT(TxnOffsetCommitRequest.schemaVersions(),
        TxnOffsetCommitResponse.schemaVersions(),
        RecordBatch.MAGIC_VALUE_V2),
    DESCRIBE_ACLS(DescribeAclsRequest.schemaVersions(),
        DescribeAclsResponse.schemaVersions()),
    CREATE_ACLS(CreateAclsRequest.schemaVersions(),
        CreateAclsRequest.schemaVersions()),
    DELETE_ACLS(DeleteAclsRequest.schemaVersions(),
        DeleteAclsResponse.schemaVersions()),
    DESCRIBE_CONFIGS(DescribeConfigsRequest.schemaVersions(),
        DescribeAclsResponse.schemaVersions()),
    ALTER_CONFIGS(AlterConfigsRequest.schemaVersions(),
        AlterConfigsResponse.schemaVersions()),
    ALTER_REPLICA_LOG_DIRS(AlterReplicaLogDirsRequest.schemaVersions(),
        AlterReplicaLogDirsResponse.schemaVersions()),
    DESCRIBE_LOG_DIRS(DescribeLogDirsRequest.schemaVersions(),
        DescribeAclsResponse.schemaVersions()),
    SASL_AUTHENTICATE(),
    CREATE_PARTITIONS(CreatePartitionsRequest.schemaVersions(),
        CreatePartitionsResponse.schemaVersions()),
    CREATE_DELEGATION_TOKEN(),
    RENEW_DELEGATION_TOKEN(),
    EXPIRE_DELEGATION_TOKEN(),
    DESCRIBE_DELEGATION_TOKEN(),
    DELETE_GROUPS(),
    ELECT_LEADERS(),
    INCREMENTAL_ALTER_CONFIGS(),
    ALTER_PARTITION_REASSIGNMENTS(),
    LIST_PARTITION_REASSIGNMENTS();

    private static final Map<Integer, ApiKeys> ID_TO_API_KEY;

    static {
        HashMap<Integer, ApiKeys> map = new HashMap<>();
        for (ApiKeys key : ApiKeys.values()) {
            map.put(Integer.valueOf(key.id), key);
        }
        ID_TO_API_KEY = Collections.unmodifiableMap(map);
    }

    private final ApiMessageType type;

    /** the permanent and immutable id of an API--this can't change ever */
    public final short id;

    /** an english description of the api--this is for debugging and can change */
    public final String name;

    /** indicates if this is a ClusterAction request used only by brokers */
    public final boolean clusterAction;

    /** indicates the minimum required inter broker magic required to support the API */
    public final byte minRequiredInterBrokerMagic;

    public final Schema[] requestSchemas;
    public final Schema[] responseSchemas;
    public final boolean requiresDelayedAllocation;

    ApiKeys() {
        this(null, null, RecordBatch.MAGIC_VALUE_V0, false);
    }

    ApiKeys(boolean clusterAction) {
        this(null, null, RecordBatch.MAGIC_VALUE_V0, clusterAction);
    }

    ApiKeys(Schema[] requestSchemas, Schema[] responseSchemas) {
        this(requestSchemas, responseSchemas, RecordBatch.MAGIC_VALUE_V0, false);
    }

    ApiKeys(Schema[] requestSchemas, Schema[] responseSchemas, boolean clusterAction) {
        this(requestSchemas, responseSchemas, RecordBatch.MAGIC_VALUE_V0, clusterAction);
    }

    ApiKeys(Schema[] requestSchemas, Schema[] responseSchemas, byte minRequiredInterBrokerMagic) {
        this(requestSchemas, responseSchemas, minRequiredInterBrokerMagic, false);
    }

    ApiKeys(Schema[] requestSchemas,
            Schema[] responseSchemas,
            byte minRequiredInterBrokerMagic,
            boolean clusterAction) {
        this.type = ApiMessageType.fromApiName(this.name());
        this.id = type.apiKey();
        this.name = type.description();
        this.clusterAction = clusterAction;
        this.minRequiredInterBrokerMagic = minRequiredInterBrokerMagic;
        this.requestSchemas = (requestSchemas == null) ? type.requestSchemas() : requestSchemas;
        this.responseSchemas = (responseSchemas == null) ? type.responseSchemas() : responseSchemas;
        boolean requestRetainsBufferReference = false;
        for (Schema requestVersionSchema : this.requestSchemas) {
            if (retainsBufferReference(requestVersionSchema)) {
                requestRetainsBufferReference = true;
                break;
            }
        }
        this.requiresDelayedAllocation = requestRetainsBufferReference;
    }

    public static ApiKeys forId(int id) {
        ApiKeys apiKey = ID_TO_API_KEY.get(Integer.valueOf(id));
        if (apiKey == null) {
            throw new IllegalArgumentException("Unexpected ApiKey " + id);
        }
        return apiKey;
    }

    public static boolean hasId(int id) {
        return ID_TO_API_KEY.containsKey(Integer.valueOf(id));
    }

    public ApiMessageType apiMessageType() {
        return type;
    }

    public short latestVersion() {
        return type.highestSupportedVersion();
    }

    public short oldestVersion() {
        return type.lowestSupportedVersion();
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
        if (this == API_VERSIONS) {
            // Fallback to version 0 for ApiVersions response. If a client sends an ApiVersionsRequest
            // using a version higher than that supported by the broker, a version 0 response is sent
            // to the client indicating UNSUPPORTED_VERSION.
            return parseResponse(version, buffer, (short) 0);
        } else {
            return responseSchema(version).read(buffer);
        }
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
