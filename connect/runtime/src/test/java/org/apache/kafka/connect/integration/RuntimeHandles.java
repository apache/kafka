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
package org.apache.kafka.connect.integration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A singleton class which provides a shared class for {@link ConnectorHandle}s and {@link TaskHandle}s that are
 * required for integration tests.
 */
public class RuntimeHandles {

    private static final RuntimeHandles INSTANCE = new RuntimeHandles();

    private final Map<String, ConnectorHandle> connectorHandles = new ConcurrentHashMap<>();

    private RuntimeHandles() {
    }

    /**
     * @return the shared {@link RuntimeHandles} instance.
     */
    public static RuntimeHandles get() {
        return INSTANCE;
    }

    /**
     * Get or create a connector handle for a given connector name. The connector need not be running at the time
     * this method is called. Once the connector is created, it will bind to this handle. Binding happens with the
     * connectorName.
     *
     * @param connectorName the name of the connector
     * @return a non-null {@link ConnectorHandle}
     */
    public ConnectorHandle connectorHandle(String connectorName) {
        return connectorHandles.computeIfAbsent(connectorName, k -> new ConnectorHandle(connectorName));
    }

    /**
     * Delete the connector handle for this connector name.
     *
     * @param connectorName name of the connector
     */
    public void deleteConnector(String connectorName) {
        connectorHandles.remove(connectorName);
    }

}
