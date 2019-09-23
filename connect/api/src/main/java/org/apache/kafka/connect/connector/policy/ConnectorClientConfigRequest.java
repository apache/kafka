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

package org.apache.kafka.connect.connector.policy;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.health.ConnectorType;

import java.util.Map;

public class ConnectorClientConfigRequest {

    private Map<String, Object> clientProps;
    private ClientType  clientType;
    private String connectorName;
    private ConnectorType connectorType;
    private Class<? extends Connector> connectorClass;

    public ConnectorClientConfigRequest(
        String connectorName,
        ConnectorType connectorType,
        Class<? extends Connector> connectorClass,
        Map<String, Object> clientProps,
        ClientType clientType) {
        this.clientProps = clientProps;
        this.clientType = clientType;
        this.connectorName = connectorName;
        this.connectorType = connectorType;
        this.connectorClass = connectorClass;
    }

    /**
     * Provides Config with prefix {@code producer.override.} for {@link ConnectorType#SOURCE}.
     * Provides Config with prefix {@code consumer.override.} for {@link ConnectorType#SINK}.
     * Provides Config with prefix {@code producer.override.} for {@link ConnectorType#SINK} for DLQ.
     * Provides Config with prefix {@code admin.override.} for {@link ConnectorType#SINK} for DLQ.
     *
     * @return The client properties specified in the Connector Config with prefix {@code producer.override.} ,
     * {@code consumer.override.} and {@code admin.override.}. The configs don't include the prefixes.
     */
    public Map<String, Object> clientProps() {
        return clientProps;
    }

    /**
     * {@link ClientType#PRODUCER} for {@link ConnectorType#SOURCE}
     * {@link ClientType#CONSUMER} for {@link ConnectorType#SINK}
     * {@link ClientType#PRODUCER} for DLQ in {@link ConnectorType#SINK}
     * {@link ClientType#ADMIN} for DLQ  Topic Creation in {@link ConnectorType#SINK}
     *
     * @return enumeration specifying the client type that is being overriden by the worker; never null.
     */
    public ClientType clientType() {
        return clientType;
    }

    /**
     * Name of the connector specified in the connector config.
     *
     * @return name of the connector; never null.
     */
    public String connectorName() {
        return connectorName;
    }

    /**
     * Type of the Connector.
     *
     * @return enumeration specifying the type of the connector {@link ConnectorType#SINK} or {@link ConnectorType#SOURCE}.
     */
    public ConnectorType connectorType() {
        return connectorType;
    }

    /**
     * The class of the Connector.
     *
     * @return the class of the Connector being created; never null
     */
    public Class<? extends Connector> connectorClass() {
        return connectorClass;
    }

    public enum ClientType {
        PRODUCER, CONSUMER, ADMIN;
    }
}