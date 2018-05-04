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

package org.apache.kafka.connect.rest.extension;

import org.apache.kafka.connect.rest.extension.entities.ConnectorHealth;

import java.util.Collection;

/**
 * Provides the ability to lookup Connector metadata and its health. This is made available to
 * the {@link ConnectRestExtension} implementations. Framework provides the implementation for
 * this interface.
 */
public interface ConnectClusterState {

    /**
     * Get a list of connector names currently running in this cluster. This is a full list of
     * connectors in the cluster gathered from the current configuration.
     */
    Collection<String> connectors();

    /**
     * Lookup the current health of a connector and its tasks.
     *
     * @param connName name of the connector
     */
    ConnectorHealth connectorHealth(String connName);
}
