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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;

import java.util.Map;

final class Kip219ClientThrottleVersion {

    // Each value is the first version of the corresponding API after KIP-219 was introduced. Remove an entry when
    // there are no supported versions below the boundary.
    static final Map<ApiKeys, Short> BOUNDARIES = Map.ofEntries(
        Map.entry(ApiKeys.PRODUCE, (short) 6),
        Map.entry(ApiKeys.FETCH, (short) 8),
        Map.entry(ApiKeys.LIST_OFFSETS, (short) 3),
        Map.entry(ApiKeys.METADATA, (short) 6),
        Map.entry(ApiKeys.OFFSET_COMMIT, (short) 4),
        Map.entry(ApiKeys.OFFSET_FETCH, (short) 4),
        Map.entry(ApiKeys.FIND_COORDINATOR, (short) 2),
        Map.entry(ApiKeys.JOIN_GROUP, (short) 3),
        Map.entry(ApiKeys.HEARTBEAT, (short) 2),
        Map.entry(ApiKeys.LEAVE_GROUP, (short) 2),
        Map.entry(ApiKeys.SYNC_GROUP, (short) 2),
        Map.entry(ApiKeys.DESCRIBE_GROUPS, (short) 2),
        Map.entry(ApiKeys.LIST_GROUPS, (short) 2),
        Map.entry(ApiKeys.API_VERSIONS, (short) 2),
        Map.entry(ApiKeys.CREATE_TOPICS, (short) 3),
        Map.entry(ApiKeys.DELETE_TOPICS, (short) 2),
        Map.entry(ApiKeys.DELETE_RECORDS, (short) 1),
        Map.entry(ApiKeys.INIT_PRODUCER_ID, (short) 1),
        Map.entry(ApiKeys.ADD_PARTITIONS_TO_TXN, (short) 1),
        Map.entry(ApiKeys.ADD_OFFSETS_TO_TXN, (short) 1),
        Map.entry(ApiKeys.END_TXN, (short) 1),
        Map.entry(ApiKeys.TXN_OFFSET_COMMIT, (short) 1),
        Map.entry(ApiKeys.DESCRIBE_CONFIGS, (short) 2),
        Map.entry(ApiKeys.ALTER_CONFIGS, (short) 1),
        Map.entry(ApiKeys.CREATE_PARTITIONS, (short) 1),
        Map.entry(ApiKeys.DELETE_GROUPS, (short) 1)
    );

    private Kip219ClientThrottleVersion() {}
}
