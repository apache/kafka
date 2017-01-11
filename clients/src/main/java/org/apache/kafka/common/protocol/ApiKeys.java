/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

/**
 * Identifiers for all the Kafka APIs
 */
public enum ApiKeys {
    PRODUCE(0, "Produce"),
    FETCH(1, "Fetch"),
    LIST_OFFSETS(2, "Offsets"),
    METADATA(3, "Metadata"),
    LEADER_AND_ISR(4, "LeaderAndIsr"),
    STOP_REPLICA(5, "StopReplica"),
    UPDATE_METADATA_KEY(6, "UpdateMetadata"),
    CONTROLLED_SHUTDOWN_KEY(7, "ControlledShutdown"),
    OFFSET_COMMIT(8, "OffsetCommit"),
    OFFSET_FETCH(9, "OffsetFetch"),
    GROUP_COORDINATOR(10, "GroupCoordinator"),
    JOIN_GROUP(11, "JoinGroup"),
    HEARTBEAT(12, "Heartbeat"),
    LEAVE_GROUP(13, "LeaveGroup"),
    SYNC_GROUP(14, "SyncGroup"),
    DESCRIBE_GROUPS(15, "DescribeGroups"),
    LIST_GROUPS(16, "ListGroups"),
    SASL_HANDSHAKE(17, "SaslHandshake"),
    API_VERSIONS(18, "ApiVersions"),
    CREATE_TOPICS(19, "CreateTopics"),
    DELETE_TOPICS(20, "DeleteTopics");

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

    ApiKeys(int id, String name) {
        this.id = (short) id;
        this.name = name;
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
            b.append(key.name);
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
