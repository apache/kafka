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

package org.apache.kafka.common.requests;

public enum ResourceType {
    UNKNOWN((byte) 0), ANY((byte) 1), TOPIC((byte) 2), GROUP((byte) 3), BROKER((byte) 4);

    private static final ResourceType[] VALUES = values();

    private final byte id;

    ResourceType(byte id) {
        this.id = id;
    }

    public byte id() {
        return id;
    }

    public static ResourceType forId(byte id) {
        if (id < 0)
            throw new IllegalArgumentException("id should be positive, id: " + id);
        if (id >= VALUES.length)
            return UNKNOWN;
        return VALUES[id];
    }
}
