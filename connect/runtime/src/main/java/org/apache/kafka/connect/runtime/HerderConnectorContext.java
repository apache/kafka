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

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConnectorContext for use with a Herder
 */
public class HerderConnectorContext implements CloseableConnectorContext {

    private static final Logger log = LoggerFactory.getLogger(HerderConnectorContext.class);

    private final AbstractHerder herder;
    private final String connectorName;
    private volatile boolean closed;

    public HerderConnectorContext(AbstractHerder herder, String connectorName) {
        this.herder = herder;
        this.connectorName = connectorName;
        this.closed = false;
    }

    @Override
    public void requestTaskReconfiguration() {
        if (closed) {
            throw new ConnectException("The request for task reconfiguration has been rejected " 
                    + "because this instance of the connector '" + connectorName + "' has already " 
                    + "been shut down.");
        }

        // Local herder runs in memory in this process
        // Distributed herder will forward the request to the leader if needed
        herder.requestTaskReconfiguration(connectorName);
    }

    @Override
    public void raiseError(Exception e) {
        if (closed) {
            log.warn("Connector {} attempted to raise error after shutdown:", connectorName, e);
            throw new ConnectException("The request to fail the connector has been rejected " 
                    + "because this instance of the connector '" + connectorName + "' has already " 
                    + "been shut down.");
        }

        herder.onFailure(connectorName, e);
    }

    @Override
    public void close() {
        closed = true;
    }
}
