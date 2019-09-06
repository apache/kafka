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

package org.apache.kafka.trogdor.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A response from the Trogdor Agent/Coordinator about how long it has been running
 */
public class UptimeResponse extends Message {

    private long serverStartMs;
    private long nowMs;

    @JsonCreator
    public UptimeResponse(@JsonProperty("serverStartMs") long serverStartMs,
                          @JsonProperty("nowMs") long nowMs) {
        this.serverStartMs = serverStartMs;
        this.nowMs = nowMs;
    }

    @JsonProperty
    public long serverStartMs() {
        return serverStartMs;
    }

    @JsonProperty
    public long nowMs() {
        return nowMs;
    }
}
