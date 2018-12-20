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

public enum ConnectProtocolCompatibility {
    STRICT {
        @Override
        String protocol() {
            return "default";
        }
    },

    COMPAT {
        @Override
        String protocol() {
            return "compat";
        }
    },

    COOP {
        @Override
        String protocol() {
            return "coop";
        }
    };

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

    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    abstract String protocol();
}
