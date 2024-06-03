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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.connect.runtime.TargetState;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class CreateConnectorRequest {
    private final String name;
    private final Map<String, String> config;
    private final InitialState initialState;

    @JsonCreator
    public CreateConnectorRequest(@JsonProperty("name") String name, @JsonProperty("config") Map<String, String> config,
                                  @JsonProperty("initial_state") InitialState initialState) {
        this.name = name;
        this.config = config;
        this.initialState = initialState;
    }

    @JsonProperty
    public String name() {
        return name;
    }

    @JsonProperty
    public Map<String, String> config() {
        return config;
    }

    @JsonProperty
    public InitialState initialState() {
        return initialState;
    }

    public TargetState initialTargetState() {
        if (initialState != null) {
            return initialState.toTargetState();
        } else {
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateConnectorRequest that = (CreateConnectorRequest) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(config, that.config) &&
            Objects.equals(initialState, that.initialState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, config, initialState);
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
            switch (this) {
                case RUNNING:
                    return TargetState.STARTED;
                case PAUSED:
                    return TargetState.PAUSED;
                case STOPPED:
                    return TargetState.STOPPED;
                default:
                    throw new IllegalArgumentException("Unknown initial state: " + this);
            }
        }
    }
}
