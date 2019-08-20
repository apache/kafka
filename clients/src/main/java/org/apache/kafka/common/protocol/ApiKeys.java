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
    PRODUCE(0),
    FETCH(1),
    LIST_OFFSETS(2),
    METADATA(3),
    LEADER_AND_ISR(4),
    STOP_REPLICA(5),
    UPDATE_METADATA(6),
    CONTROLLED_SHUTDOWN(7),
    OFFSET_COMMIT(8),
    OFFSET_FETCH(9),
    FIND_COORDINATOR(10),
    JOIN_GROUP(11),
    HEARTBEAT(12),
    LEAVE_GROUP(13),
    SYNC_GROUP(14),
    DESCRIBE_GROUPS(15),
    LIST_GROUPS(16),
    SASL_HANDSHAKE(17),
    API_VERSIONS(18),
    CREATE_TOPICS(19),
    DELETE_TOPICS(20),
    DELETE_RECORDS(21),
    INIT_PRODUCER_ID(22),
    OFFSET_FOR_LEADER_EPOCH(23),
    ADD_PARTITIONS_TO_TXN(24),
    ADD_OFFSETS_TO_TXN(25),
    END_TXN(26, false, RecordBatch.MAGIC_VALUE_V2),
    WRITE_TXN_MARKERS(27, true, RecordBatch.MAGIC_VALUE_V2),
    TXN_OFFSET_COMMIT(28, false, RecordBatch.MAGIC_VALUE_V2),
    DESCRIBE_ACLS(29),
    CREATE_ACLS(30),
    DELETE_ACLS(31),
    DESCRIBE_CONFIGS(32),
    ALTER_CONFIGS(33),
    ALTER_REPLICA_LOG_DIRS(34),
    DESCRIBE_LOG_DIRS(35),
    SASL_AUTHENTICATE(36),
    CREATE_PARTITIONS(37),
    CREATE_DELEGATION_TOKEN(38),
    RENEW_DELEGATION_TOKEN(39),
    EXPIRE_DELEGATION_TOKEN(40),
    DESCRIBE_DELEGATION_TOKEN(41),
    DELETE_GROUPS(42),
    ELECT_LEADERS(43),
    INCREMENTAL_ALTER_CONFIGS(44),
    ALTER_PARTITION_REASSIGNMENTS(45),
    LIST_PARTITION_REASSIGNMENTS(46);

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

    ApiKeys(int id) {
        this(id, false, RecordBatch.MAGIC_VALUE_V0);
    }

    ApiKeys(int id, boolean clusterAction) {
        this(id, clusterAction, RecordBatch.MAGIC_VALUE_V0);
    }

    ApiKeys(int id, boolean clusterAction, byte minRequiredInterBrokerMagic) {
        if ((id < 0) || (id > 0x7fff)) {
            throw new IllegalArgumentException("invalid api key " + id);
        }
        this.type = ApiMessageType.fromApiKey((short) id);
        this.id = type.apiKey();
        this.name = type.description();
        this.clusterAction = clusterAction;
        this.minRequiredInterBrokerMagic = minRequiredInterBrokerMagic;
        this.requestSchemas = type.requestSchemas();
        this.responseSchemas = type.responseSchemas();
        boolean requestRetainsBufferReference = false;
        for (Schema requestVersionSchema : requestSchemas) {
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
