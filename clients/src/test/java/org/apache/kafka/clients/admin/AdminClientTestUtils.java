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

import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;

public class AdminClientTestUtils {

    /**
     * Helper to create a ListPartitionReassignmentsResult instance for a given Throwable.
     * ListPartitionReassignmentsResult's constructor is only accessible from within the
     * admin package.
     */
    public static ListPartitionReassignmentsResult listPartitionReassignmentsResult(Throwable t) {
        KafkaFutureImpl<Map<TopicPartition, PartitionReassignment>> future = new KafkaFutureImpl<>();
        future.completeExceptionally(t);
        return new ListPartitionReassignmentsResult(future);
    }
}
