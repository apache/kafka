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
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.Collection;

/**
 * Listener for rebalance events in the worker group.
 */
public interface WorkerRebalanceListener {
    /**
     * Invoked when a new assignment is created by joining the Connect worker group. This is invoked for both successful
     * and unsuccessful assignments.
     */
    void onAssigned(ConnectProtocol.Assignment assignment, int generation);

    /**
     * Invoked when a rebalance operation starts, revoking ownership for the set of connectors and tasks.
     */
    void onRevoked(String leader, Collection<String> connectors, Collection<ConnectorTaskId> tasks);
}
