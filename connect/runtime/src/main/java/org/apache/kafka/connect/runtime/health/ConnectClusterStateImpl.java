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

package org.apache.kafka.connect.runtime.health;

import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.health.ConnectorHealth;
import org.apache.kafka.connect.health.ConnectorState;
import org.apache.kafka.connect.health.ConnectorType;
import org.apache.kafka.connect.health.TaskState;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.Callback;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectClusterStateImpl implements ConnectClusterState {

    private Herder herder;

    public ConnectClusterStateImpl(Herder herder) {
        this.herder = herder;
    }

    @Override
    public Collection<String> connectors() {
        final Collection<String> connectors = new ArrayList<>();
        herder.connectors(new Callback<java.util.Collection<String>>() {
            @Override
            public void onCompletion(Throwable error, Collection<String> result) {
                connectors.addAll(result);
            }
        });
        return connectors;
    }

    @Override
    public ConnectorHealth connectorHealth(String connName) {

        ConnectorStateInfo state = herder.connectorStatus(connName);
        ConnectorState connectorState = new ConnectorState(
            state.connector().state(),
            state.connector().workerId(),
            state.connector().trace()
        );
        Map<Integer, TaskState> taskStates = taskStates(state.tasks());
        ConnectorHealth connectorHealth = new ConnectorHealth(
            connName,
            connectorState,
            taskStates,
            ConnectorType.valueOf(state.type().name())
        );
        return connectorHealth;
    }

    private Map<Integer, TaskState> taskStates(List<ConnectorStateInfo.TaskState> states) {

        Map<Integer, TaskState> taskStates = new HashMap<>();

        for (ConnectorStateInfo.TaskState state : states) {
            taskStates.put(
                state.id(),
                new TaskState(state.id(), state.workerId(), state.state(), state.trace())
            );
        }
        return taskStates;
    }
}
