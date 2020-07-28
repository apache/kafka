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

/**
 * Representation of a SASL/SCRAM Mechanism.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API">KIP-554: Add Broker-side SCRAM Config API</a>
 */
public enum ScramMechanism {
    UNKNOWN((byte) 0),
    SCRAM_SHA_256((byte) 1),
    SCRAM_SHA_512((byte) 2);

    /**
     *
     * @param b the byte representation
     * @return the instance corresponding to the given byte value, otherwise {@link #UNKNOWN}
     */
    public static ScramMechanism from(byte b) {
        for (ScramMechanism scramMechanism : ScramMechanism.values()) {
            if (scramMechanism.type == b) {
                return scramMechanism;
            }
        }
        return UNKNOWN;
    }

    /**
     *
     * @return the corresponding SASL SCRAM mechanism name
     * @see <a href="https://tools.ietf.org/html/rfc5802#section-4>
     *     Salted Challenge Response Authentication Mechanism (SCRAM) SASL and GSS-API Mechanisms, Section 4</a>
     */
    public String toMechanismName() {
        return toString().replace('_', '-');
    }

    byte type;

    private ScramMechanism(byte type) {
        this.type = type;
    }
}
