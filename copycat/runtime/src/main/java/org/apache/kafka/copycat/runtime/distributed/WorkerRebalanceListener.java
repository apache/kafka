/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.runtime.distributed;

import org.apache.kafka.copycat.util.ConnectorTaskId;

import java.util.Collection;

/**
 * Listener for rebalance events in the worker group. This is only invoked when rebalancing and assignment is successful.
 */
public interface WorkerRebalanceListener {
    /**
     * Invoked when a new, valid assignment is created by joining the Copycat worker group. Note that this is not
     * invoked when if a join group cycle completes but assignment failed (e.g. due to config offset mismatches).
     */
    void onAssigned(long configOffset, String leader, Collection<String> connectors, Collection<ConnectorTaskId> tasks);

    /**
     * Invoked when a rebalance operation starts, revoking ownership for the set of
     */
    void onRevoked(String leader, Collection<String> connectors, Collection<ConnectorTaskId> tasks);
}
