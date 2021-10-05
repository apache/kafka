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

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum LeaderAndIsrRequestType {
    UNKNOWN_TYPE((byte) -1, "Unknown type"),
    INCREMENTAL((byte) 0, "A LeaderAndIsrRequest that is not guaranteed to contain all topic partitions assigned to a broker."),
    FULL((byte) 1, "A full LeaderAndIsrRequest containing all partitions the broker is a replica for.");

    private static final Map<Byte, LeaderAndIsrRequestType> CODE_TO_TYPE = new HashMap<>();

    static {
        for (LeaderAndIsrRequestType type : LeaderAndIsrRequestType.values()) {
            if (CODE_TO_TYPE.put(type.code(), type) != null) {
                throw new ExceptionInInitializerError("Code " + type.code() + " for LeaderAndIsrRequestType " +
                    "has already been used");
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(LeaderAndIsrRequestType.class);

    private final byte code;
    private final String description;

    LeaderAndIsrRequestType(byte code, String description) {
        this.code = code;
        this.description = description;
    }

    public byte code() {
        return this.code;
    }

    public String description() {
        return this.description;
    }

    public static LeaderAndIsrRequestType forCode(byte code) {
        LeaderAndIsrRequestType type = CODE_TO_TYPE.get(code);
        if (type != null) {
            return type;
        } else {
            log.warn("Unexpected code for LeaderAndIsrRequestType: {}", code);
            return UNKNOWN_TYPE;
        }
    }
}
