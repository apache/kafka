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

package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.admin.DecommissionBrokerOptions;
import org.apache.kafka.common.internals.KafkaFutureImpl;

/**
 * Context class to encapsulate parameters of a call to decommission a broker.
 *
 * @param <T> The type of the return value of the KafkaFuture
 */
public final class DecommissionBrokerOperationContext<T> {
    final private DecommissionBrokerOptions options;
    final private long deadline;
    final private int brokerId;
    final private KafkaFutureImpl<T> future;

    public DecommissionBrokerOperationContext(DecommissionBrokerOptions options,
                                              long deadline,
                                              int brokerId,
                                              KafkaFutureImpl<T> future) {
        this.options = options;
        this.deadline = deadline;
        this.brokerId = brokerId;
        this.future = future;
    }

    public DecommissionBrokerOptions options() {
        return options;
    }

    public long deadline() {
        return deadline;
    }

    public int brokerId() {
        return brokerId;
    }

    public KafkaFutureImpl<T> future() {
        return future;
    }
}
