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
package org.apache.kafka.connect.runtime.distributed;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * An enumeration of the modes available to the worker to signal which Connect protocols are
 * enabled at any time.
 *
 * {@code EAGER} signifies that this worker only supports prompt release of assigned connectors
 * and tasks in every rebalance. Corresponds to Connect protocol V0.
 *
 * {@code COOPERATIVE} signifies that this worker only supports release and acquisition of
 * connectors and tasks based on policy that applies incremental and cooperative rebalancing.
 * Corresponds to Connect protocol V1 or greater.
 *
 * {@code COMPATIBLE} signifies that this worker supports both eager and cooperative Connect
 * protocols and will use the version that is elected by the Kafka broker coordinator during
 * rebalancing.
 */
public enum ConnectProtocolCompatibility {
    EAGER {
        @Override
        String protocol() {
            return "default";
        }
    },

    COMPATIBLE {
        @Override
        String protocol() {
            return "compatible";
        }
    },

    COOPERATIVE {
        @Override
        String protocol() {
            return "cooperative";
        }
    };

    // Support both lower case and upper case values
    private static final Map<String, ConnectProtocolCompatibility> REVERSE = new HashMap<>(values().length * 2);

    static {
        for (ConnectProtocolCompatibility mode : values()) {
            REVERSE.put(mode.name(), mode);
            REVERSE.put(mode.name().toLowerCase(Locale.ROOT), mode);
        }
    }

    public static ConnectProtocolCompatibility compatibility(String name) {
        ConnectProtocolCompatibility compat = REVERSE.get(name);
        if (compat == null) {
            throw new IllegalArgumentException("Unknown Connect protocol compatibility mode: " + name);
        }
        return compat;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * Return the name of the protocol that this mode will use in {@code ProtocolMetadata}.
     *
     * @return the protocol name
     */
    abstract String protocol();
}
