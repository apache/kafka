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

package org.apache.kafka.clients.admin;

/**
 * A listing of a groups topic partition listing in the cluster.
 */
public class GroupOffsetListing {
    private final Long offset;
    private final Long timestamp;

    /**
     * Create an instance with the specified parameters.
     *
     * @param offset    The topic partition offset
     * @param timestamp The topic partition timestamp
     */
    public GroupOffsetListing(Long offset, Long timestamp) {
        this.offset = offset;
        this.timestamp = timestamp;
    }

    /**
     * The offset of the groups topic partition.
     */
    public Long offset() {
        return offset;
    }

    /**
     * The timestamp of the groups topic partition.
     */
    public Long timestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "(offset=" + offset + ", " +
            "timestamp=" + timestamp + ")";
    }
}
