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
package org.apache.kafka.coordinator.transaction;


import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.NodeToControllerChannelManager;

import java.util.function.Supplier;

/**
 * ProducerIdManager is the part of the transaction coordinator that provides ProducerIds in a unique way
 * such that the same producerId will not be assigned twice across multiple transaction coordinators.
 * <p>
 * ProducerIds are managed by the controller. When requesting a new range of IDs, we are guaranteed to receive
 * a unique block.
 */
public interface ProducerIdManager {

    long generateProducerId() throws Exception;

    static ProducerIdManager rpc(int brokerId,
                                 Time time,
                                 Supplier<Long> brokerEpochSupplier,
                                 NodeToControllerChannelManager controllerChannel) {
        return new RPCProducerIdManager(brokerId, time, brokerEpochSupplier, controllerChannel);
    }
}
