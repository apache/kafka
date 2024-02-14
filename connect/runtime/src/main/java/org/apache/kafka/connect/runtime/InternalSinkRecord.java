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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.runtime.errors.ProcessingContext;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A specialization of {@link SinkRecord} that allows a {@link WorkerSinkTask} to track the
 * original {@link ConsumerRecord} for each {@link SinkRecord}. It is used internally and not
 * exposed to connectors.
 */
public class InternalSinkRecord extends SinkRecord {

    private final ProcessingContext<ConsumerRecord<byte[], byte[]>> context;

    public InternalSinkRecord(ProcessingContext<ConsumerRecord<byte[], byte[]>> context, SinkRecord record) {
        super(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                record.valueSchema(), record.value(), record.kafkaOffset(), record.timestamp(),
                record.timestampType(), record.headers(), context.original().topic(),
                context.original().partition(), context.original().offset());
        this.context = context;
    }

    protected InternalSinkRecord(ProcessingContext<ConsumerRecord<byte[], byte[]>> context, String topic,
                                 int partition, Schema keySchema, Object key, Schema valueSchema,
                                 Object value, long kafkaOffset, Long timestamp,
                                 TimestampType timestampType, Iterable<Header> headers) {
        super(topic, partition, keySchema, key, valueSchema, value, kafkaOffset, timestamp, timestampType, headers,
                context.original().topic(), context.original().partition(),
                context.original().offset());
        this.context = context;
    }

    @Override
    public SinkRecord newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key,
                                Schema valueSchema, Object value, Long timestamp,
                                Iterable<Header> headers) {
        return new InternalSinkRecord(context, topic, kafkaPartition, keySchema, key,
                valueSchema, value, kafkaOffset(), timestamp, timestampType(), headers);
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * Return the context used to process this record
     *
     * @return the processing context; never null
     */
    public ProcessingContext<ConsumerRecord<byte[], byte[]>> context() {
        return context;
    }
}

