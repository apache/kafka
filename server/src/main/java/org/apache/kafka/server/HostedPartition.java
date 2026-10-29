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

package org.apache.kafka.server;

import java.util.Optional;

/**
 * Sealed interface to represent the state of hosted partitions. We create a concrete (active) Partition
 * instance when the broker receives a LeaderAndIsr request from the controller or a metadata
 * log record from the Quorum controller indicating that the broker should be either a leader
 * or follower of a partition.
 */
public sealed interface HostedPartition<T> {
    /**
     * This broker does not have any state for this partition locally.
     */
    record None<T>() implements HostedPartition<T> { }
    /**
     * This broker hosts the partition and it is online.
     */
    record Online<T>(T partition) implements HostedPartition<T> { }
    /**
     * This broker hosts the partition, but it is in an offline log directory.
     */
    record Offline<T>(Optional<T> partition) implements HostedPartition<T> { }
}
