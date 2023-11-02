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
 *  This exception is raised for any error that occurs while deserializing records received by the consumer using 
 *  the configured {@link org.apache.kafka.common.serialization.Deserializer}.
 */
public class RecordDeserializationException extends SerializationException {

    private static final long serialVersionUID = 1L;
    private final TopicPartition partition;
    private final long offset;

    public RecordDeserializationException(TopicPartition partition, long offset, String message, Throwable cause) {
        super(message, cause);
        this.partition = partition;
        this.offset = offset;
    }

    public TopicPartition topicPartition() {
        return partition;
    }

    public long offset() {
        return offset;
    }
}
