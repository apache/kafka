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

import org.apache.kafka.common.TopicPartition;

/**
 *  Any exception encountered on an invalid record (deserialization error, corrupt record, etc...)
 */
public class RecordDeserializationException extends SerializationException {

    private static final long serialVersionUID = 1L;
    private TopicPartition partition;
    private long offset;

    public RecordDeserializationException(TopicPartition partition, long offset, String message) {
        this(partition, offset, message, null);
    }

    public RecordDeserializationException(TopicPartition partition, long offset, String message, Throwable cause) {
        super(message, cause);
        this.partition = partition;
        this.offset = offset;
    }

    public TopicPartition partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    /* avoid the expensive and useless stack trace for deserialization exceptions */
    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
