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

package org.apache.kafka.metadata;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.HashMap;
import java.util.Map;

/**
 * The broker state.
 *
 * The numeric values used here are part of Kafka's public API.  They appear in metrics,
 * and are also sent over the wire in some cases.
 *
 * For the legacy broker, the expected state transitions are:
 *
 *                +-----------+
 *                |NOT_RUNNING|
 *                +-----+-----+
 *                      |
 *                      v
 *                +-----+-----+
 *                |REGISTERING+--+
 *                +-----+-----+  | +----+------------+
 *                      |        +-+RecoveringFrom   |
 *                      v          |UncleanShutdown  |
 *               +-------+-------+ +-------+---------+
 *               | RUNNING       |            |
 *               +-------+-------+<-----------+
 *                       |
 *                       v
 *                +-----+------------+
 *                |PendingControlled |
 *                |Shutdown          |
 *                +-----+------------+
 *                      |
 *                      v
 *               +-----+----------+
 *               |BrokerShutting  |
 *               |Down            |
 *               +-----+----------+
 *                     |
 *                     v
 *               +-----+-----+
 *               |Not Running|
 *               +-----------+
 */
@InterfaceStability.Evolving
public enum BrokerState {
    /**
     * The state the broker is in when it first starts up, and hasn't acquired a
     * lease.
     */
    NOT_RUNNING((byte) 0),

    /**
     * The state the broker is in when it is registering with the active controller.
     * Note: in pre-500-brokers this state was previously known as Starting.
     */
    REGISTERING((byte) 1),

    /**
     * The state the broker is in when it has a lease, but is still recovering
     * from an unclean shutdown.
     */
    RECOVERING_FROM_UNCLEAN_SHUTDOWN((byte) 2),

    /**
     * The state the broker is in when it is running and accepting client
     * requests.
     */
    RUNNING((byte) 3),

    /**
     * The state the broker is in when it is fenced.
     */
    FENCED((byte) 4),

    /**
     * The state the broker is in when it is attempting to perform a controlled
     * shutdown.
     */
    PENDING_CONTROLLED_SHUTDOWN((byte) 6),

    /**
     * The state the broker is in when it is shutting down.
     */
    SHUTTING_DOWN((byte) 7),

    /**
     * The broker is in an unknown state.
     */
    UNKNOWN((byte) 127);

    private final static Map<Byte, BrokerState> VALUES_TO_ENUMS = new HashMap<>();

    static {
        for (BrokerState state : BrokerState.values()) {
            VALUES_TO_ENUMS.put(state.value(), state);
        }
    }

    private final byte value;

    BrokerState(byte value) {
        this.value = value;
    }

    public static BrokerState fromValue(byte value) {
        BrokerState state = VALUES_TO_ENUMS.get(value);
        if (state == null) {
            return UNKNOWN;
        }
        return state;
    }

    public byte value() {
        return value;
    }
}
