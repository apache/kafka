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

package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.trogdor.task.TaskController;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * The specification for a task which connects and disconnects many times a
 * second to stress the broker.
 */
public class ConnectionStressSpec extends TaskSpec {
    private final List<String> clientNodes;
    private final String bootstrapServers;
    private final Map<String, String> commonClientConf;
    private final int targetConnectionsPerSec;
    private final int numThreads;
    private final ConnectionStressAction action;

    enum ConnectionStressAction {
        CONNECT,
        FETCH_METADATA
    }

    @JsonCreator
    public ConnectionStressSpec(@JsonProperty("startMs") long startMs,
            @JsonProperty("durationMs") long durationMs,
            @JsonProperty("clientNode") List<String> clientNodes,
            @JsonProperty("bootstrapServers") String bootstrapServers,
            @JsonProperty("commonClientConf") Map<String, String> commonClientConf,
            @JsonProperty("targetConnectionsPerSec") int targetConnectionsPerSec,
            @JsonProperty("numThreads") int numThreads,
            @JsonProperty("action") ConnectionStressAction action) {
        super(startMs, durationMs);
        this.clientNodes = (clientNodes == null) ? Collections.emptyList() :
            Collections.unmodifiableList(new ArrayList<>(clientNodes));
        this.bootstrapServers = (bootstrapServers == null) ? "" : bootstrapServers;
        this.commonClientConf = configOrEmptyMap(commonClientConf);
        this.targetConnectionsPerSec = targetConnectionsPerSec;
        this.numThreads = numThreads < 1 ? 1 : numThreads;
        this.action = (action == null) ? ConnectionStressAction.CONNECT : action;
    }

    @JsonProperty
    public List<String> clientNode() {
        return clientNodes;
    }

    @JsonProperty
    public String bootstrapServers() {
        return bootstrapServers;
    }

    @JsonProperty
    public Map<String, String> commonClientConf() {
        return commonClientConf;
    }

    @JsonProperty
    public int targetConnectionsPerSec() {
        return targetConnectionsPerSec;
    }

    @JsonProperty
    public int numThreads() {
        return numThreads;
    }

    @JsonProperty
    public ConnectionStressAction action() {
        return action;
    }

    public TaskController newController(String id) {
        return topology -> new TreeSet<>(clientNodes);
    }

    @Override
    public TaskWorker newTaskWorker(String id) {
        return new ConnectionStressWorker(id, this);
    }
}
