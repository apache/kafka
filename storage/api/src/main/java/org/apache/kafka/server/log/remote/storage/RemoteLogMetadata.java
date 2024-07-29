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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Base class for remote log metadata objects like {@link RemoteLogSegmentMetadata}, {@link RemoteLogSegmentMetadataUpdate},
 * and {@link RemotePartitionDeleteMetadata}.
 */
@InterfaceStability.Evolving
public abstract class RemoteLogMetadata {

    /**
     * Broker id from which this event is generated.
     */
    private final int brokerId;

    /**
     * Epoch time in milliseconds at which this event is generated.
     */
    private final long eventTimestampMs;

    protected RemoteLogMetadata(int brokerId, long eventTimestampMs) {
        this.brokerId = brokerId;
        this.eventTimestampMs = eventTimestampMs;
    }

    /**
     * @return Epoch time in milliseconds at which this event is occurred.
     */
    public long eventTimestampMs() {
        return eventTimestampMs;
    }

    /**
     * @return Broker id from which this event is generated.
     */
    public int brokerId() {
        return brokerId;
    }

    /**
     * @return TopicIdPartition for which this event is generated.
     */
    public abstract TopicIdPartition topicIdPartition();
}
