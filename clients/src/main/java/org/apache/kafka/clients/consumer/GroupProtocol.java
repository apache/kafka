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

package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.annotation.InterfaceAudience;

import java.util.Locale;

/**
 * Enum representing the supported consumer group protocols.
 * <ul>
 *     <li>{@link #CLASSIC} - The Classic consumer group protocol (pre KIP-848)</li>
 *     <li>{@link #CONSUMER} - The Consumer rebalance protocol (KIP-848)</li>
 * </ul>
 */
@InterfaceAudience.Public
public enum GroupProtocol {
    /** Classic group protocol.  */
    CLASSIC("CLASSIC"),

    /** Consumer group protocol */
    CONSUMER("CONSUMER");

    /**
     * String representation of the group protocol.
     */
    public final String name;

    GroupProtocol(final String name) {
        this.name = name;
    }

    /**
     * Case-insensitive group protocol lookup by string name.
     *
     * @param name The name of the group protocol
     * @return The corresponding GroupProtocol
     * @throws IllegalArgumentException If the name does not match any protocol
     */
    public static GroupProtocol of(final String name) {
        return GroupProtocol.valueOf(name.toUpperCase(Locale.ROOT));
    }
}
