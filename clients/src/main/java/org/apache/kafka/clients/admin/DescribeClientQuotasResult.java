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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;

import java.util.Map;

/**
 * The result of the {@link Admin#describeClientQuotas(ClientQuotaFilter, DescribeClientQuotasOptions)} call.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class DescribeClientQuotasResult {

    private final KafkaFuture<Map<ClientQuotaEntity, Map<String, Double>>> entities;

    /**
     * Maps an entity to its configured quota value(s). Note if no value is defined for a quota
     * type for that entity's config, then it is not included in the resulting value map.
     *
     * @param entities future for the collection of entities that matched the filter
     */
    public DescribeClientQuotasResult(KafkaFuture<Map<ClientQuotaEntity, Map<String, Double>>> entities) {
        this.entities = entities;
    }

    /**
     * Returns a map from quota entity to a future which can be used to check the status of the operation.
     */
    public KafkaFuture<Map<ClientQuotaEntity, Map<String, Double>>> entities() {
        return entities;
    }
}
