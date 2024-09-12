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

/**
 * The result of {@link org.apache.kafka.clients.admin.Admin#removeRaftVoter(int, org.apache.kafka.common.Uuid, org.apache.kafka.clients.admin.RemoveRaftVoterOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Stable
public class RemoveRaftVoterResult {
    private final KafkaFuture<Void> result;

    RemoveRaftVoterResult(KafkaFuture<Void> result) {
        this.result = result;
    }

    /**
     * Returns a future that completes when the voter has been removed.
     */
    public KafkaFuture<Void> all() {
        return result;
    }

}
