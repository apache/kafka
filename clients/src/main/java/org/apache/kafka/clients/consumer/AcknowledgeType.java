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
 * The acknowledge type is used with {@link KafkaShareConsumer#acknowledge(ConsumerRecord, AcknowledgeType)} to indicate
 * whether the record was consumed successfully.
 */
@InterfaceAudience.Public
public enum AcknowledgeType {
    /** The record was consumed successfully. */
    ACCEPT((byte) 1),

    /** The record was not consumed successfully. Release it for another delivery attempt. */
    RELEASE((byte) 2),

    /** The record was not consumed successfully. Reject it and do not release it for another delivery attempt. */
    REJECT((byte) 3),

    /** The record is still being processed. Renew the acquisition lock so processing can continue. */
    RENEW((byte) 4);

    /** The unique identifier for this acknowledge type. */
    public final byte id;

    AcknowledgeType(byte id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return super.toString().toLowerCase(Locale.ROOT);
    }

    /**
     * Returns the AcknowledgeType for the given identifier.
     *
     * @param id The identifier for the acknowledge type
     * @return The corresponding AcknowledgeType
     * @throws IllegalArgumentException If the ID is not recognized
     */
    public static AcknowledgeType forId(byte id) {
        switch (id) {
            case 1:
                return ACCEPT;
            case 2:
                return RELEASE;
            case 3:
                return REJECT;
            case 4:
                return RENEW;
            default:
                throw new IllegalArgumentException("Unknown acknowledge type id: " + id);
        }
    }
}
