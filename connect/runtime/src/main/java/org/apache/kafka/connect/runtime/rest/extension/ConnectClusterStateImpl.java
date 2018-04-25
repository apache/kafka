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

package org.apache.kafka.connect.runtime.rest.extension;

import org.apache.kafka.connect.rest.extension.ConnectClusterState;
import org.apache.kafka.connect.rest.extension.entities.ConnectorStateDetail;
import org.apache.kafka.connect.rest.extension.entities.ConnectorType;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.Callback;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
    public ConnectorStateDetail connectorStatus(String connName) {

        ConnectorStateInfo connectorStateFromHerder = herder.connectorStatus(connName);
        ConnectorStateDetail.ConnectorState connectorState =
            new ConnectorStateDetail.ConnectorState(
                connectorStateFromHerder.connector().state(),
                connectorStateFromHerder.connector().workerId(),
                connectorStateFromHerder.connector().trace()
            );
        List<ConnectorStateDetail.TaskState> taskStateList =
            taskStates(connectorStateFromHerder.tasks());
        ConnectorStateDetail connectorStateDetail = new ConnectorStateDetail(
            connName,
            connectorState,
            taskStateList,
            ConnectorType.valueOf(connectorStateFromHerder.type().name())
        );
        return connectorStateDetail;
    }

    private List<ConnectorStateDetail.TaskState> taskStates(
        List<ConnectorStateInfo.TaskState> taskStatesFromHerder) {

        List<ConnectorStateDetail.TaskState> taskStates = new ArrayList<>();

        for (ConnectorStateInfo.TaskState taskStateFromHerder : taskStatesFromHerder) {
            taskStates.add(new ConnectorStateDetail.TaskState(
                               taskStateFromHerder.id(),
                               taskStateFromHerder.workerId(),
                               taskStateFromHerder.state(),
                               taskStateFromHerder.trace()
                           )
            );
        }
        return taskStates;
    }
}
