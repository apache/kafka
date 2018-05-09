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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.connector.ConnectorContext;

/**
 * ConnectorContext for use with a Herder
 */
public class HerderConnectorContext implements ConnectorContext {

    private final AbstractHerder herder;
    private final String connectorName;

    public HerderConnectorContext(AbstractHerder herder, String connectorName) {
        this.herder = herder;
        this.connectorName = connectorName;
    }

    @Override
    public void requestTaskReconfiguration() {
        // Local herder runs in memory in this process
        // Distributed herder will forward the request to the leader if needed
        herder.requestTaskReconfiguration(connectorName);
    }

    @Override
    public void raiseError(Exception e) {
        herder.onFailure(connectorName, e);
    }
}
