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
package org.apache.kafka.common.record;

import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;

/**
 * Utility class for easy interaction with control records.
 */
public class ControlRecordUtils {

    public static final short LEADER_CHANGE_SCHEMA_VERSION = new LeaderChangeMessage().highestSupportedVersion();

    public static LeaderChangeMessage deserializeLeaderChangeMessage(Record record) {
        ControlRecordType recordType = ControlRecordType.parse(record.key());
        if (recordType != ControlRecordType.LEADER_CHANGE) {
            throw new IllegalArgumentException(
                "Expected LEADER_CHANGE control record type(3), but found " + recordType.toString());
        }
        return deserializeLeaderChangeMessage(record.value().duplicate());
    }

    public static LeaderChangeMessage deserializeLeaderChangeMessage(ByteBuffer data) {
        ByteBufferAccessor byteBufferAccessor = new ByteBufferAccessor(data.duplicate());
        return new LeaderChangeMessage(byteBufferAccessor, LEADER_CHANGE_SCHEMA_VERSION);
    }
}
