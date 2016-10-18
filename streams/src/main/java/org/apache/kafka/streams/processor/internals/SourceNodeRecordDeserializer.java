/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.errors.StreamsException;

import static java.lang.String.format;

class SourceNodeRecordDeserializer implements RecordDeserializer {
    private final SourceNode sourceNode;

    SourceNodeRecordDeserializer(final SourceNode sourceNode) {
        this.sourceNode = sourceNode;
    }

    @Override
    public ConsumerRecord<Object, Object> deserialize(final ConsumerRecord<byte[], byte[]> rawRecord) {
        final Object key;
        try {
            key = sourceNode.deserializeKey(rawRecord.topic(), rawRecord.key());
        } catch (Exception e) {
            throw new StreamsException(format("Failed to deserialize key for record. topic=%s, partition=%d, offset=%d",
                                              rawRecord.topic(), rawRecord.partition(), rawRecord.offset()), e);
        }

        final Object value;
        try {
            value = sourceNode.deserializeValue(rawRecord.topic(), rawRecord.value());
        } catch (Exception e) {
            throw new StreamsException(format("Failed to deserialize value for record. topic=%s, partition=%d, offset=%d",
                                              rawRecord.topic(), rawRecord.partition(), rawRecord.offset()), e);
        }

        return new ConsumerRecord<>(rawRecord.topic(), rawRecord.partition(), rawRecord.offset(),
                                    rawRecord.timestamp(), TimestampType.CREATE_TIME,
                                    rawRecord.checksum(),
                                    rawRecord.serializedKeySize(),
                                    rawRecord.serializedValueSize(), key, value);

    }
}
