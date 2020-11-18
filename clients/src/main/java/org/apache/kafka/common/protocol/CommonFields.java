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
    public static final Field.Int32 LEADER_EPOCH = new Field.Int32("leader_epoch", "The leader epoch");
    public static final Field.Int32 CURRENT_LEADER_EPOCH = new Field.Int32("current_leader_epoch",
            "The current leader epoch, if provided, is used to fence consumers/replicas with old metadata. " +
                    "If the epoch provided by the client is larger than the current epoch known to the broker, then " +
                    "the UNKNOWN_LEADER_EPOCH error code will be returned. If the provided epoch is smaller, then " +
                    "the FENCED_LEADER_EPOCH error code will be returned.");
}
