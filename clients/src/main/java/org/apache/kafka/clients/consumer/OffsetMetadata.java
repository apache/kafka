/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;

/**
 * The metadata for an offset commit that has been acknowledged by the server
 */
public final class OffsetMetadata {

    private final Map<TopicPartition, Long> offsets;
    private final Map<TopicPartition, RuntimeException> errors;
    
    public OffsetMetadata(Map<TopicPartition, Long> offsets, Map<TopicPartition, RuntimeException> errors) {
        super();
        this.offsets = offsets;
        this.errors = errors;
    }

    public OffsetMetadata(Map<TopicPartition, Long> offsets) {
        this(offsets, null);
    }

    /**
     * The offset of the record in the topic/partition.
     */
    public long offset(TopicPartition partition) {
        if(this.errors != null)
            throw errors.get(partition);
        return offsets.get(partition);
    }

    /**
     * @return The exception corresponding to the error code returned by the server
     */
    public Exception error(TopicPartition partition) {
        if(errors != null)
            return errors.get(partition);
        else
            return null;
    }
}
