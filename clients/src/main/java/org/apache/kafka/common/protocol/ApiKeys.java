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

import org.apache.kafka.common.ApiKey;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;

/**
 * Identifiers for all the Kafka APIs
 */
public class ApiKeys {
    static {
        EnumMap<ApiKey, Info> info = new EnumMap<ApiKey, Info>(ApiKey.class);
        info.put(ApiKey.PRODUCE, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.FETCH, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.LIST_OFFSETS, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.METADATA, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.LEADER_AND_ISR, new Info(true, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.STOP_REPLICA, new Info(true, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.UPDATE_METADATA_KEY, new Info(true, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.CONTROLLED_SHUTDOWN_KEY, new Info(true, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.OFFSET_COMMIT, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.OFFSET_FETCH, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.FIND_COORDINATOR, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.JOIN_GROUP, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.HEARTBEAT, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.LEAVE_GROUP, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.SYNC_GROUP, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.DESCRIBE_GROUPS, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.LIST_GROUPS, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.SASL_HANDSHAKE, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.API_VERSIONS, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.CREATE_TOPICS, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.DELETE_TOPICS, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.DELETE_RECORDS, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.INIT_PRODUCER_ID, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.OFFSET_FOR_LEADER_EPOCH, new Info(true, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.ADD_PARTITIONS_TO_TXN, new Info(false, RecordBatch.MAGIC_VALUE_V2));
        info.put(ApiKey.ADD_OFFSETS_TO_TXN, new Info(false, RecordBatch.MAGIC_VALUE_V2));
        info.put(ApiKey.END_TXN, new Info(false, RecordBatch.MAGIC_VALUE_V2));
        info.put(ApiKey.WRITE_TXN_MARKERS, new Info(true, RecordBatch.MAGIC_VALUE_V2));
        info.put(ApiKey.TXN_OFFSET_COMMIT, new Info(false, RecordBatch.MAGIC_VALUE_V2));
        info.put(ApiKey.DESCRIBE_ACLS, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.CREATE_ACLS, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.DELETE_ACLS, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.DESCRIBE_CONFIGS, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        info.put(ApiKey.ALTER_CONFIGS, new Info(false, RecordBatch.MAGIC_VALUE_V0));
        INFO = info;
    }

    private final static Map<ApiKey, Info> INFO;

    public static class Info {
        /** indicates if this is a ClusterAction request used only by brokers */
        public final boolean clusterAction;

        /** indicates the minimum required inter broker magic required to support the API */
        public final byte minRequiredInterBrokerMagic;

        Info(boolean clusterAction, byte minRequiredInterBrokerMagic) {
            this.clusterAction = clusterAction;
            this.minRequiredInterBrokerMagic = minRequiredInterBrokerMagic;
        }

        public boolean clusterAction() {
            return clusterAction;
        }

        public byte minRequiredInterBrokerMagic() {
            return minRequiredInterBrokerMagic;
        }
    }

    public static Info info(ApiKey api) {
        return INFO.get(api);
    }

    public static Schema requestSchema(ApiKey api, short version) {
        return schemaFor(api, Protocol.REQUESTS, version);
    }

    public static Schema responseSchema(ApiKey api, short version) {
        return schemaFor(api, Protocol.RESPONSES, version);
    }

    public static Struct parseRequest(ApiKey api, short version, ByteBuffer buffer) {
        return requestSchema(api, version).read(buffer);
    }

    public static Struct parseResponse(ApiKey api, short version, ByteBuffer buffer) {
        if (api == ApiKey.API_VERSIONS) {
            // Fallback to version 0 for ApiVersions response. If a client sends an ApiVersionsRequest
            // using a version higher than that supported by the broker, a version 0 response is sent
            // to the client indicating UNSUPPORTED_VERSION.
            return parseResponse(api, version, buffer, (short) 0);
        } else {
            return responseSchema(api, version).read(buffer);
        }
    }

    protected static Struct parseResponse(ApiKey api, short version, ByteBuffer buffer, short fallbackVersion) {
        int bufferPosition = buffer.position();
        try {
            return responseSchema(api, version).read(buffer);
        } catch (SchemaException e) {
            if (version != fallbackVersion) {
                buffer.position(bufferPosition);
                return responseSchema(api, fallbackVersion).read(buffer);
            } else
                throw e;
        }
    }

    private static Schema schemaFor(ApiKey api, Schema[][] schemas, short version) {
        if (api.id() > schemas.length)
            throw new IllegalArgumentException("No schema available for API key " + api);
        if (version < 0 || version > api.supportedRange().highest())
            throw new IllegalArgumentException("Invalid version for API key " + api + ": " + version);

        Schema[] versions = schemas[api.id()];
        if (versions[version] == null)
            throw new IllegalArgumentException("Unsupported version for API key " + api + ": " + version);
        return versions[version];
    }

    private static String toHtml() {
        final StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>");
        b.append("<th>Name</th>\n");
        b.append("<th>Key</th>\n");
        b.append("</tr>");
        for (ApiKey api : ApiKey.values()) {
            b.append("<tr>\n");
            b.append("<td>");
            b.append("<a href=\"#The_Messages_" + api.title() + "\">" + api.title() + "</a>");
            b.append("</td>");
            b.append("<td>");
            b.append(api.id());
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
