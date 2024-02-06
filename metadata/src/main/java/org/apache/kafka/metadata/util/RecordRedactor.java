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

package org.apache.kafka.metadata.util;

import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.metadata.KafkaConfigSchema;


/**
 * Converts a metadata record to a string suitable for logging to slf4j.
 * This means that passwords and key material are omitted from the output.
 */
public final class RecordRedactor {
    private final KafkaConfigSchema configSchema;

    public RecordRedactor(KafkaConfigSchema configSchema) {
        this.configSchema = configSchema;
    }

    public String toLoggableString(ApiMessage message) {
        MetadataRecordType type = MetadataRecordType.fromId(message.apiKey());
        switch (type) {
            case CONFIG_RECORD: {
                if (!configSchema.isSensitive((ConfigRecord) message)) {
                    return message.toString();
                }
                ConfigRecord duplicate = ((ConfigRecord) message).duplicate();
                duplicate.setValue("(redacted)");
                return duplicate.toString();
            }
            case USER_SCRAM_CREDENTIAL_RECORD: {
                UserScramCredentialRecord record = (UserScramCredentialRecord) message;
                return "UserScramCredentialRecord("
                        + "name=" + ((record.name() == null) ? "null" : "'" + record.name() + "'")
                        + ", mechanism=" + record.mechanism()
                        + ", salt=(redacted)"
                        + ", storedKey=(redacted)"
                        + ", serverKey=(redacted)"
                        + ", iterations=" + record.iterations()
                        + ")";
            }
            default:
                return message.toString();
        }
    }
}
