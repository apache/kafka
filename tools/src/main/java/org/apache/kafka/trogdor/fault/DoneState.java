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

package org.apache.kafka.trogdor.fault;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The state a fault is in on the agent or controller when it is completed,
 * either normally or with an error.
 */
public class DoneState extends FaultState {
    private final long doneMs;
    private final String errorStr;

    @JsonCreator
    public DoneState(@JsonProperty("doneMs") long doneMs,
                     @JsonProperty("errorStr") String errorStr) {
        this.doneMs = doneMs;
        this.errorStr = errorStr;
    }

    @JsonProperty
    public long doneMs() {
        return doneMs;
    }

    @JsonProperty
    public String errorStr() {
        return errorStr;
    }
}
