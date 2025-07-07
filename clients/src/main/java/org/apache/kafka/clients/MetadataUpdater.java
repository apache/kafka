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
package org.apache.kafka.clients;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

/**
 * The interface used by `NetworkClient` to request cluster metadata info to be updated and to retrieve the cluster nodes
 * from such metadata. This is an internal class.
 * <p>
 * This class is not thread-safe!
 */
public interface MetadataUpdater extends Closeable {

    /**
     * Gets the current cluster info without blocking.
     */
    List<Node> fetchNodes();

    /**
     * Returns true if an update to the cluster metadata info is due.
     */
    boolean isUpdateDue(long now);

    /**
     * Starts a cluster metadata update if needed and possible. Returns the time until the metadata update (which would
     * be 0 if an update has been started as a result of this call).
     *
     * If the implementation relies on `NetworkClient` to send requests, `handleSuccessfulResponse` will be
     * invoked after the metadata response is received.
     *
     * The semantics of `needed` and `possible` are implementation-dependent and may take into account a number of
     * factors like node availability, how long since the last metadata update, etc.
     */
    long maybeUpdate(long now);

    /**
     * Handle a server disconnect.
     *
     * This provides a mechanism for the `MetadataUpdater` implementation to use the NetworkClient instance for its own
     * requests with special handling for disconnections of such requests.
     *
     * @param now Current time in milliseconds
     * @param nodeId The id of the node that disconnected
     * @param maybeAuthException Optional authentication error
     */
    void handleServerDisconnect(long now, String nodeId, Optional<AuthenticationException> maybeAuthException);

    /**
     * Handle a metadata request failure.
     *
     * @param now Current time in milliseconds
     * @param maybeFatalException Optional fatal error (e.g. {@link UnsupportedVersionException})
     */
    void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException);

    /**
     * Handle responses for metadata requests.
     *
     * This provides a mechanism for the `MetadataUpdater` implementation to use the NetworkClient instance for its own
     * requests with special handling for completed receives of such requests.
     */
    void handleSuccessfulResponse(RequestHeader requestHeader, long now, MetadataResponse metadataResponse);

    /**
     * Returns true if metadata couldn't be fetched for `rebootstrapTriggerMs` or if server requested rebootstrap.
     *
     * @param now Current time in milliseconds
     * @param rebootstrapTriggerMs Configured timeout after which rebootstrap is triggered
     */
    default boolean needsRebootstrap(long now, long rebootstrapTriggerMs) {
        return false;
    }

    /**
     * Performs rebootstrap, replacing the existing cluster with the bootstrap cluster.
     *
     * @param now Current time in milliseconds
     */
    default void rebootstrap(long now) {}

    /**
     * Close this updater.
     */
    @Override
    void close();
}
