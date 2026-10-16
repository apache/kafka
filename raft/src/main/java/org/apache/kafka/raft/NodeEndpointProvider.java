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
package org.apache.kafka.raft;

/**
 * An interface for looking up the registered endpoints of a node by its node ID.
 *
 * <p>This is used by the Raft layer to derive controller endpoints from an external
 * registry (such as the cluster metadata image) when processing
 * {@code AddRaftVoterRequest} v2+ with an empty listener set.
 */
@FunctionalInterface
public interface NodeEndpointProvider {

    /**
     * Returns the {@link Endpoints} registered for the given node ID, or
     * {@link Endpoints#empty()} if the node is not known.
     *
     * @param nodeId the ID of the node whose endpoints are requested
     */
    Endpoints endpointsOf(int nodeId);

    /**
     * A no-op provider that always returns {@link Endpoints#empty()}.
     */
    NodeEndpointProvider NOOP = __ -> Endpoints.empty();
}
