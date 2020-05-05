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

import org.apache.kafka.common.message.LeaderChangeMessageData;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

/**
 * Utility class for easy interaction with control records.
 */
public class ControlRecordUtils {

    public static LeaderChangeMessageData deserialize(Record record) {
        ControlRecordType recordType = ControlRecordType.parse(record.key());
        if (recordType != ControlRecordType.LEADER_CHANGE) {
            throw new IllegalArgumentException(
                "Expected LEADER_CHANGE control record type(3), but found " + recordType.toString());
        }
        return deserialize(record.value().duplicate());
    }

    public static LeaderChangeMessageData deserialize(ByteBuffer data) {
        LeaderChangeMessageData leaderChangeMessage = new LeaderChangeMessageData();
        Struct leaderChangeMessageStruct = LeaderChangeMessageData.SCHEMAS[leaderChangeMessage.highestSupportedVersion()]
                                               .read(data);
        leaderChangeMessage.fromStruct(leaderChangeMessageStruct, leaderChangeMessage.highestSupportedVersion());
        return leaderChangeMessage;
    }
}
