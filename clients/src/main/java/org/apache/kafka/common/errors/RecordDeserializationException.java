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
package org.apache.kafka.common.errors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import java.nio.ByteBuffer;

/**
 *  This exception is raised for any error that occurs while deserializing records received by the consumer using 
 *  the configured {@link org.apache.kafka.common.serialization.Deserializer}.
 */
public class RecordDeserializationException extends SerializationException {

    private static final long serialVersionUID = 2L;

    public enum DeserializationExceptionOrigin {
        KEY,
        VALUE
    }

    private final DeserializationExceptionOrigin origin;
    private final TopicPartition partition;
    private final long offset;
    private final TimestampType timestampType;
    private final long timestamp;
    private final ByteBuffer keyBuffer;
    private final ByteBuffer valueBuffer;
    private final Headers headers;

    /**
     * @deprecated Since 3.9. Use {@link #RecordDeserializationException(DeserializationExceptionOrigin, TopicPartition, long, long, TimestampType, ByteBuffer, ByteBuffer, Headers, String, Throwable)} instead.
     */
    @Deprecated
    public RecordDeserializationException(TopicPartition partition,
                                          long offset,
                                          String message,
                                          Throwable cause) {
        super(message, cause);
        this.origin = null;
        this.partition = partition;
        this.offset = offset;
        this.timestampType = TimestampType.NO_TIMESTAMP_TYPE;
        this.timestamp = ConsumerRecord.NO_TIMESTAMP;
        this.keyBuffer = null;
        this.valueBuffer = null;
        this.headers = null;
    }

    public RecordDeserializationException(DeserializationExceptionOrigin origin,
                                          TopicPartition partition,
                                          long offset,
                                          long timestamp,
                                          TimestampType timestampType,
                                          ByteBuffer keyBuffer,
                                          ByteBuffer valueBuffer,
                                          Headers headers,
                                          String message,
                                          Throwable cause) {
        super(message, cause);
        this.origin = origin;
        this.offset = offset;
        this.timestampType = timestampType;
        this.timestamp = timestamp;
        this.partition = partition;
        this.keyBuffer = keyBuffer;
        this.valueBuffer = valueBuffer;
        this.headers = headers;
    }

    public DeserializationExceptionOrigin origin() {
        return origin;
    }

    public TopicPartition topicPartition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public TimestampType timestampType() {
        return timestampType;
    }

    public long timestamp() {
        return timestamp;
    }

    public ByteBuffer keyBuffer() {
        return keyBuffer;
    }

    public ByteBuffer valueBuffer() {
        return valueBuffer;
    }

    public Headers headers() {
        return headers;
    }
}
