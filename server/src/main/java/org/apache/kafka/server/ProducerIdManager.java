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


import org.apache.kafka.common.utils.Time;

import java.util.function.Supplier;

/**
 * ProducerIdManager is the part of the transaction coordinator that provides ProducerIds in a unique way
 * such that the same producerId will not be assigned twice across multiple transaction coordinators.
 * <p>
 * ProducerIds are managed by the controller. When requesting a new range of IDs, we are guaranteed to receive
 * a unique block.
 */
public abstract class ProducerIdManager {

    public static final int RETRY_BACKOFF_MS = 50;
    // Once we reach this percentage of PIDs consumed from the current block, trigger a fetch of the next block
    protected static final double PID_PREFETCH_THRESHOLD = 0.90;
    protected static final int ITERATION_LIMIT = 3;
    protected static final long NO_RETRY = -1L;

    public abstract Long generateProducerId() throws Exception;

    // For testing purposes
    public abstract boolean hasValidBlock();

    public void shutdown() {
    }

    public static ProducerIdManager rpc(int brokerId,
                                        Time time,
                                        Supplier<Long> brokerEpochSupplier,
                                        NodeToControllerChannelManager controllerChannel) {
        return new RPCProducerIdManager(brokerId, time, brokerEpochSupplier, controllerChannel);
    }
}
