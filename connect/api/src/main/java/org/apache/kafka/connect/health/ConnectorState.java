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

package org.apache.kafka.connect.health;

/**
 * Describes the status, worker ID, and any errors associated with a connector.
 */
public class ConnectorState extends AbstractState {

    /**
     * Provides an instance of the ConnectorState.
     *
     * @param state - the status of connector, may not be {@code null} or empty
     * @param workerId - the workerId associated with the connector, may not be {@code null} or empty
     * @param traceMessage - any error message associated with the connector, may be {@code null} or empty
     */
    public ConnectorState(String state, String workerId, String traceMessage) {
        super(state, workerId, traceMessage);
    }

    @Override
    public String toString() {
        return "ConnectorState{"
            + "state='" + state() + '\''
            + ", traceMessage='" + traceMessage() + '\''
            + ", workerId='" + workerId() + '\''
            + '}';
    }
}
