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
package org.apache.kafka.streams.query;


public enum FailureReason {
    /**
     * Failure indicating that the store doesn't know how to handle the given query.
     */
    UNKNOWN_QUERY_TYPE,

    /**
     * Failure indicating that the store partition is not (yet) up to the desired bound.
     * The caller should either try again later or try a different replica.
     */
    NOT_UP_TO_BOUND,

    /**
     * Failure indicating that the requested store partition is not present on the local
     * KafkaStreams instance. It may have been migrated to another instance during a rebalance.
     * The caller is recommended to try a different replica.
     */
    NOT_PRESENT,

    /**
     * The requested store partition does not exist at all. For example, partition 4 was requested,
     * but the store in question only has 4 partitions (0 through 3).
     */
    DOES_NOT_EXIST;
}