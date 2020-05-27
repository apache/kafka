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
package org.apache.kafka.connect.sink;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;

/**
 * SinkRecord is a {@link ConnectRecord} that has been read from Kafka and includes the kafkaOffset of
 * the record in the Kafka topic-partition in addition to the standard fields. This information
 * should be used by the SinkTask to coordinate kafkaOffset commits.
 *
 * It also includes the {@link TimestampType}, which may be {@link TimestampType#NO_TIMESTAMP_TYPE}, and the relevant
 * timestamp, which may be {@code null}.
 */
public class SinkRecord extends ConnectRecord<SinkRecord> {
    private final long kafkaOffset;
    private final TimestampType timestampType;

    public SinkRecord(String topic, int partition, Schema keySchema, Object key, Schema valueSchema, Object value, long kafkaOffset) {
        this(topic, partition, keySchema, key, valueSchema, value, kafkaOffset, null, TimestampType.NO_TIMESTAMP_TYPE);
    }

    public SinkRecord(String topic, int partition, Schema keySchema, Object key, Schema valueSchema, Object value, long kafkaOffset,
                      Long timestamp, TimestampType timestampType) {
        this(topic, partition, keySchema, key, valueSchema, value, kafkaOffset, timestamp, timestampType, null);
    }

    public SinkRecord(String topic, int partition, Schema keySchema, Object key, Schema valueSchema, Object value, long kafkaOffset,
                      Long timestamp, TimestampType timestampType, Iterable<Header> headers) {
        super(topic, partition, keySchema, key, valueSchema, value, timestamp, headers);
        this.kafkaOffset = kafkaOffset;
        this.timestampType = timestampType;
    }

    public long kafkaOffset() {
        return kafkaOffset;
    }

    public TimestampType timestampType() {
        return timestampType;
    }

    @Override
    public SinkRecord newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value, Long timestamp) {
        return newRecord(topic, kafkaPartition, keySchema, key, valueSchema, value, timestamp, headers().duplicate());
    }

    @Override
    public SinkRecord newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value,
                                Long timestamp, Iterable<Header> headers) {
        return new SinkRecord(topic, kafkaPartition, keySchema, key, valueSchema, value, kafkaOffset(), timestamp, timestampType, headers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        SinkRecord that = (SinkRecord) o;

        if (kafkaOffset != that.kafkaOffset)
            return false;

        return timestampType == that.timestampType;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Long.hashCode(kafkaOffset);
        result = 31 * result + timestampType.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SinkRecord{" +
                "kafkaOffset=" + kafkaOffset +
                ", timestampType=" + timestampType +
                "} " + super.toString();
    }
}
