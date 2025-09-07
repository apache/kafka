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
package org.apache.kafka.connect.runtime.rest.entities;

import org.apache.kafka.connect.runtime.TargetState;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Locale;
import java.util.Map;

public record CreateConnectorRequest(
    @JsonProperty("name") String name,
    @JsonProperty("config") Map<String, String> config,
    @JsonProperty("initial_state") InitialState initialState
) {
    public TargetState initialTargetState() {
        return initialState != null ? initialState.toTargetState() : null;
    }

    public enum InitialState {
        RUNNING,
        PAUSED,
        STOPPED;

        @JsonCreator
        public static InitialState forValue(String value) {
            return InitialState.valueOf(value.toUpperCase(Locale.ROOT));
        }

        public TargetState toTargetState() {
            return switch (this) {
                case RUNNING -> TargetState.STARTED;
                case PAUSED  -> TargetState.PAUSED;
                case STOPPED -> TargetState.STOPPED;
            };
        }
    }
}