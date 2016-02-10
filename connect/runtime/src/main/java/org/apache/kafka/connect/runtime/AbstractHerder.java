/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Abstract Herder implementation which handles connector/task lifecycle tracking.
 */
public abstract class AbstractHerder implements Herder, TaskStatus.Listener, ConnectorStatus.Listener {

    protected final StatusBackingStore statusBackingStore;
    private final String workerId;

    public AbstractHerder(StatusBackingStore statusBackingStore, String workerId) {
        this.statusBackingStore = statusBackingStore;
        this.workerId = workerId;
    }

    protected abstract int generation();

    protected void startServices() {
        this.statusBackingStore.start();
    }

    protected void stopServices() {
        this.statusBackingStore.stop();
    }

    @Override
    public void onStartup(String connector) {
        statusBackingStore.put(connector, new ConnectorStatus(ConnectorStatus.State.RUNNING,
                workerId, generation()));
    }

    @Override
    public void onShutdown(String connector) {
        statusBackingStore.putSafe(connector, new ConnectorStatus(ConnectorStatus.State.UNASSIGNED,
                workerId, generation()));
    }

    @Override
    public void onFailure(String connector, Throwable t) {
        statusBackingStore.putSafe(connector, new ConnectorStatus(ConnectorStatus.State.FAILED,
                workerId, generation()));
    }

    @Override
    public void onStartup(ConnectorTaskId id) {
        statusBackingStore.put(id, new TaskStatus(TaskStatus.State.RUNNING, workerId, generation()));
    }

    @Override
    public void onFailure(ConnectorTaskId id, Throwable t) {
        statusBackingStore.putSafe(id, new TaskStatus(TaskStatus.State.FAILED, workerId, generation()));
    }

    @Override
    public void onShutdown(ConnectorTaskId id) {
        statusBackingStore.putSafe(id, new TaskStatus(TaskStatus.State.UNASSIGNED, workerId, generation()));
    }

    @Override
    public void onDeletion(String connector) {
        for (Integer task : statusBackingStore.getAll(connector).keySet()) {
            TaskStatus status = new TaskStatus(TaskStatus.State.DESTROYED, workerId, generation());
            statusBackingStore.put(new ConnectorTaskId(connector, task), status);
        }

        ConnectorStatus status = new ConnectorStatus(ConnectorStatus.State.DESTROYED, workerId, generation());
        statusBackingStore.put(connector, status);
    }

    @Override
    public void connectorStatus(String connName, Callback<ConnectorStateInfo> callback) {
        ConnectorStateInfo status = connectorStatus(connName);

        if (status == null)
            throw new NotFoundException("No status found for connector " + connName);
        else
            callback.onCompletion(null, status);
    }

    @Override
    public void taskStatus(ConnectorTaskId task, Callback<ConnectorStateInfo.TaskState> callback) {
        ConnectorStateInfo.TaskState status = taskStatus(task);
        if (status == null)
            callback.onCompletion(new NotFoundException("No status found for task " + task), null);
        else
            callback.onCompletion(null, status);
    }

    public ConnectorStateInfo connectorStatus(String connName) {
        ConnectorStatus connector = statusBackingStore.get(connName);
        if (connector == null)
            return null;

        Map<Integer, TaskStatus> tasks = statusBackingStore.getAll(connName);

        ConnectorStateInfo.ConnectorState connectorState = new ConnectorStateInfo.ConnectorState(
                connector.state().toString(), connector.workerId(), connector.msg());
        List<ConnectorStateInfo.TaskState> taskStates = new ArrayList<>();

        for (Map.Entry<Integer, TaskStatus> taskEntry : tasks.entrySet()) {
            TaskStatus status = taskEntry.getValue();
            taskStates.add(new ConnectorStateInfo.TaskState(taskEntry.getKey(),
                    status.state().toString(), status.workerId(), status.msg()));
        }

        Collections.sort(taskStates);

        return new ConnectorStateInfo(connName, connectorState, taskStates);
    }

    public ConnectorStateInfo.TaskState taskStatus(ConnectorTaskId id) {
        TaskStatus status = statusBackingStore.get(id);

        if (status == null)
            return null;

        return new ConnectorStateInfo.TaskState(id.task(), status.state().toString(),
                status.workerId(), status.msg());
    }

}
