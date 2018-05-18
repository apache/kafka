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

import org.apache.kafka.common.protocol.types.Field;

public class CommonFields {
    public static final Field.Int32 THROTTLE_TIME_MS = new Field.Int32("throttle_time_ms",
            "Duration in milliseconds for which the request was throttled due to quota violation (Zero if the " +
                    "request did not violate any quota)", 0);
    public static final Field.Str TOPIC_NAME = new Field.Str("topic", "Name of topic");
    public static final Field.Int32 PARTITION_ID = new Field.Int32("partition", "Topic partition id");
    public static final Field.Int16 ERROR_CODE = new Field.Int16("error_code", "Response error code");
    public static final Field.NullableStr ERROR_MESSAGE = new Field.NullableStr("error_message", "Response error message");
    public static final Field.Int32 LEADER_EPOCH = new Field.Int32("leader_epoch", "The epoch");

    // Group APIs
    public static final Field.Str GROUP_ID = new Field.Str("group_id", "The unique group identifier");
    public static final Field.Int32 GENERATION_ID = new Field.Int32("generation_id", "The generation of the group.");
    public static final Field.Str MEMBER_ID = new Field.Str("member_id", "The member id assigned by the group " +
            "coordinator or null if joining for the first time.");

    // Transactional APIs
    public static final Field.Str TRANSACTIONAL_ID = new Field.Str("transactional_id", "The transactional id corresponding to the transaction.");
    public static final Field.NullableStr NULLABLE_TRANSACTIONAL_ID = new Field.NullableStr("transactional_id",
            "The transactional id or null if the producer is not transactional");
    public static final Field.Int64 PRODUCER_ID = new Field.Int64("producer_id", "Current producer id in use by the transactional id.");
    public static final Field.Int16 PRODUCER_EPOCH = new Field.Int16("producer_epoch", "Current epoch associated with the producer id.");

    // ACL APIs
    public static final Field.Int8 RESOURCE_TYPE = new Field.Int8("resource_type", "The resource type");
    public static final Field.Str RESOURCE_NAME = new Field.Str("resource_name", "The resource name");
    public static final Field.NullableStr RESOURCE_NAME_FILTER = new Field.NullableStr("resource_name", "The resource name filter");
    public static final Field.Str PRINCIPAL = new Field.Str("principal", "The ACL principal");
    public static final Field.NullableStr PRINCIPAL_FILTER = new Field.NullableStr("principal", "The ACL principal filter");
    public static final Field.Str HOST = new Field.Str("host", "The ACL host");
    public static final Field.NullableStr HOST_FILTER = new Field.NullableStr("host", "The ACL host filter");
    public static final Field.Int8 OPERATION = new Field.Int8("operation", "The ACL operation");
    public static final Field.Int8 PERMISSION_TYPE = new Field.Int8("permission_type", "The ACL permission type");

    public static final Field.Str PRINCIPAL_TYPE = new Field.Str("principal_type", "principalType of the Kafka principal");
    public static final Field.Str PRINCIPAL_NAME = new Field.Str("name", "name of the Kafka principal");

}
