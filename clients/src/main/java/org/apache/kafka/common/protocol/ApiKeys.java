/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.protocol;

/**
 * Identifiers for all the Kafka APIs
 */
public enum ApiKeys {
    PRODUCE(0, "produce"),
    FETCH(1, "fetch"),
    LIST_OFFSETS(2, "list_offsets"),
    METADATA(3, "metadata"),
    LEADER_AND_ISR(4, "leader_and_isr"),
    STOP_REPLICA(5, "stop_replica"),
    OFFSET_COMMIT(6, "offset_commit"),
    OFFSET_FETCH(7, "offset_fetch");

    public static int MAX_API_KEY = 0;

    static {
        for (ApiKeys key : ApiKeys.values()) {
            MAX_API_KEY = Math.max(MAX_API_KEY, key.id);
        }
    }

    /** the perminant and immutable id of an API--this can't change ever */
    public final short id;

    /** an english description of the api--this is for debugging and can change */
    public final String name;

    private ApiKeys(int id, String name) {
        this.id = (short) id;
        this.name = name;
    }

}