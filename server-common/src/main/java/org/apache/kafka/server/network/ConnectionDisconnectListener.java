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

package org.apache.kafka.server.network;

/**
 * Listener that is invoked when a connection is disconnected. This is useful for cases where the server
 * needs to perform cleanup tasks when a connection is disconnected.
 */
public interface ConnectionDisconnectListener {

    /**
     * Invoked when a connection is disconnected.
     * <p>
     * <em>Note</em>: The method is invoked when the connection to the client is closed hence the
     * implementation of this method should not perform any blocking operations.
     *
     * @param connectionId The connection id as defined in {@link org.apache.kafka.common.requests.RequestContext}.
     *                     This id is unique for each connection.
     */
    void onDisconnect(String connectionId);
}
