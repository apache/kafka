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
 * A listing of a consumer group in the cluster.
 */
public class ConsumerGroupListing {
    private final String groupId;
    private final boolean isSimpleConsumerGroup;

    /**
     * Create an instance with the specified parameters.
     *
     * @param groupId Group Id
     * @param isSimpleConsumerGroup If consumer group is simple or not.
     */
    public ConsumerGroupListing(String groupId, boolean isSimpleConsumerGroup) {
        this.groupId = groupId;
        this.isSimpleConsumerGroup = isSimpleConsumerGroup;
    }

    /**
     * Consumer Group Id
     */
    public String groupId() {
        return groupId;
    }

    /**
     * If Consumer Group is simple or not.
     */
    public boolean isSimpleConsumerGroup() {
        return isSimpleConsumerGroup;
    }

    @Override
    public String toString() {
        return "(" +
            "groupId='" + groupId + '\'' +
            ", isSimpleConsumerGroup=" + isSimpleConsumerGroup +
            ')';
    }
}
