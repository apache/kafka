/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.security.auth;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public enum Operation {
    READ((byte) 0, "Read"),
    WRITE((byte) 1, "Write"),
    CREATE((byte) 2, "Create"),
    DELETE((byte) 3, "Delete"),
    ALTER((byte) 4, "Alter"),
    DESCRIBE((byte) 5, "Describe"),
    CLUSTER_ACTION((byte) 6, "ClusterAction"),
    ALL((byte) 7, "All");

    private static Operation[] idToOperation;
    private static Map<String, Operation> nameToOperation;
    public static final int MAX_ID;

    static {
        int maxId = -1;
        for (Operation key : Operation.values()) {
            maxId = Math.max(maxId, key.id);
        }
        idToOperation = new Operation[maxId + 1];
        nameToOperation = new HashMap<String, Operation>();
        for (Operation key : Operation.values()) {
            idToOperation[key.id] = key;
            nameToOperation.put(key.name.toUpperCase(Locale.ROOT), key);
        }
        MAX_ID = maxId;
    }

    public final byte id;
    public final String name;

    private Operation(byte id, String name) {
        this.id = id;
        this.name = name;
    }

    /** Case insensitive lookup by name */
    public static Operation fromString(String name) {
        Operation operation = nameToOperation.get(name.toUpperCase(Locale.ROOT));
        if (operation == null) {
            throw new IllegalArgumentException(String.format("No enum constant with name %s", name));
        }
        return operation;
    }

    public static Operation forId(byte id) {
        return idToOperation[id];
    }
}
