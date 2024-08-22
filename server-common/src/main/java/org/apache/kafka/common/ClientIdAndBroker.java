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
package org.apache.kafka.common;

import java.util.Objects;

/**
 * Convenience case class since (clientId, brokerInfo) pairs are used to create
 * SyncProducer Request Stats and SimpleConsumer Request and Response Stats.
 */
public class ClientIdAndBroker {
    public final String clientId;
    public final String brokerHost;
    public final int brokerPort;

    public ClientIdAndBroker(String clientId, String brokerHost, int brokerPort) {
        this.clientId = clientId;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
    }

    @Override
    public String toString() {
        return String.format("%s-%s-%d", clientId, brokerHost, brokerPort);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientIdAndBroker that = (ClientIdAndBroker) o;
        return brokerPort == that.brokerPort && Objects.equals(clientId, that.clientId) && Objects.equals(brokerHost, that.brokerHost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, brokerHost, brokerPort);
    }
}
