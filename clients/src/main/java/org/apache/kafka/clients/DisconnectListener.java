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

import org.apache.kafka.common.errors.AuthenticationException;

import java.util.Optional;

/**
 * The interface used for classes that need to respond to broker disconnections. The NetworkClient will inform
 * any registered listeners of each broker disconnections via handleServerDisconnect.
 */
public interface DisconnectListener {

    /**
     * Handle a server disconnect.
     *
     * This provides a mechanism for the `NetworkClient` instance to notify the `DisconnectListener` implementation of
     * broker disconnections. Signature identical to MetadataUpdater::handleServerDisconnect.
     *
     * @param now Current time in milliseconds
     * @param nodeId The id of the node that disconnected
     * @param maybeAuthException Optional authentication error
     */
    void handleServerDisconnect(long now, String nodeId, Optional<AuthenticationException> maybeAuthException);
}
