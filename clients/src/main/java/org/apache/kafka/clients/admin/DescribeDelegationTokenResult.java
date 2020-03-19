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

import java.util.List;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.security.token.delegation.DelegationToken;

/**
 * The result of the {@link KafkaAdminClient#describeDelegationToken(DescribeDelegationTokenOptions)} call.
 */
public class DescribeDelegationTokenResult {
    private final KafkaFuture<List<DelegationToken>> delegationTokens;

    DescribeDelegationTokenResult(KafkaFuture<List<DelegationToken>> delegationTokens) {
        this.delegationTokens = delegationTokens;
    }

    /**
     * Returns a future which yields list of delegation tokens
     */
    public KafkaFuture<List<DelegationToken>> delegationTokens() {
        return delegationTokens;
    }
}
