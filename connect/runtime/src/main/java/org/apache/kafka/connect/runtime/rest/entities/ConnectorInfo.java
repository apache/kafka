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

import org.apache.kafka.connect.util.ConnectorTaskId;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Information about a connector, returned by several REST endpoints. {@code offsetsStatus} is populated only by
 * {@code POST /connectors} when the request supplied {@code initial_offsets}, and is omitted from the response
 * otherwise, so the other endpoints returning this type are unchanged.
 */
public record ConnectorInfo(
    @JsonProperty("name") String name,
    @JsonProperty("config") Map<String, String> config,
    @JsonProperty("tasks") List<ConnectorTaskId> tasks,
    @JsonProperty("type") ConnectorType type,
    @JsonProperty("offsets_status") @JsonInclude(JsonInclude.Include.NON_NULL) String offsetsStatus
) {
    /**
     * Canonical constructor, declared explicitly and annotated so that Jackson unambiguously uses it as the creator
     * rather than having to choose between this and the convenience constructor below. Deserialization matters here
     * because a worker that is not the leader forwards requests and parses the leader's response body into this type.
     */
    @JsonCreator
    public ConnectorInfo(
        @JsonProperty("name") String name,
        @JsonProperty("config") Map<String, String> config,
        @JsonProperty("tasks") List<ConnectorTaskId> tasks,
        @JsonProperty("type") ConnectorType type,
        @JsonProperty("offsets_status") String offsetsStatus
    ) {
        this.name = name;
        this.config = config;
        this.tasks = tasks;
        this.type = type;
        this.offsetsStatus = offsetsStatus;
    }

    /**
     * Convenience constructor for the endpoints that have no offsets status to report.
     */
    public ConnectorInfo(String name, Map<String, String> config, List<ConnectorTaskId> tasks, ConnectorType type) {
        this(name, config, tasks, type, null);
    }

    /**
     * @return a copy of this instance carrying the given offsets status
     */
    public ConnectorInfo withOffsetsStatus(String offsetsStatus) {
        return new ConnectorInfo(name, config, tasks, type, offsetsStatus);
    }
}