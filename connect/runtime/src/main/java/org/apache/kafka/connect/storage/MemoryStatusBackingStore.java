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
package org.apache.kafka.connect.storage;

import org.apache.kafka.connect.runtime.ConnectorStatus;
import org.apache.kafka.connect.runtime.TaskStatus;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.Table;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MemoryStatusBackingStore implements StatusBackingStore {
    private final Table<String, Integer, TaskStatus> tasks;
    private final Map<String, ConnectorStatus> connectors;

    public MemoryStatusBackingStore() {
        this.tasks = new Table<>();
        this.connectors = new HashMap<>();
    }

    @Override
    public void configure(WorkerConfig config) {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public synchronized void put(ConnectorStatus status) {
        if (status.state() == ConnectorStatus.State.DESTROYED)
            connectors.remove(status.id());
        else
            connectors.put(status.id(), status);
    }

    @Override
    public synchronized void putSafe(ConnectorStatus status) {
        put(status);
    }

    @Override
    public synchronized void put(TaskStatus status) {
        if (status.state() == TaskStatus.State.DESTROYED)
            tasks.remove(status.id().connector(), status.id().task());
        else
            tasks.put(status.id().connector(), status.id().task(), status);
    }

    @Override
    public synchronized void putSafe(TaskStatus status) {
        put(status);
    }

    @Override
    public synchronized TaskStatus get(ConnectorTaskId id) {
        return tasks.get(id.connector(), id.task());
    }

    @Override
    public synchronized ConnectorStatus get(String connector) {
        return connectors.get(connector);
    }

    @Override
    public synchronized Collection<TaskStatus> getAll(String connector) {
        return new HashSet<>(tasks.row(connector).values());
    }

    @Override
    public synchronized Set<String> connectors() {
        return new HashSet<>(connectors.keySet());
    }

    @Override
    public void flush() {

    }

}
