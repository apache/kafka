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
package org.apache.kafka.common.replica;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.net.InetAddress;
import java.util.Objects;

/**
 * Holder for all the client metadata required to determine a preferred replica.
 */
public interface ClientMetadata {

    /**
     * Rack ID sent by the client
     */
    String rackId();

    /**
     * Client ID sent by the client
     */
    String clientId();

    /**
     * Incoming address of the client
     */
    InetAddress clientAddress();

    /**
     * Security principal of the client
     */
    KafkaPrincipal principal();

    /**
     * Listener name for the client
     */
    String listenerName();


    class DefaultClientMetadata implements ClientMetadata {
        private final String rackId;
        private final String clientId;
        private final InetAddress clientAddress;
        private final KafkaPrincipal principal;
        private final String listenerName;

        public DefaultClientMetadata(String rackId, String clientId, InetAddress clientAddress,
                                     KafkaPrincipal principal, String listenerName) {
            this.rackId = rackId;
            this.clientId = clientId;
            this.clientAddress = clientAddress;
            this.principal = principal;
            this.listenerName = listenerName;
        }

        @Override
        public String rackId() {
            return rackId;
        }

        @Override
        public String clientId() {
            return clientId;
        }

        @Override
        public InetAddress clientAddress() {
            return clientAddress;
        }

        @Override
        public KafkaPrincipal principal() {
            return principal;
        }

        @Override
        public String listenerName() {
            return listenerName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DefaultClientMetadata that = (DefaultClientMetadata) o;
            return Objects.equals(rackId, that.rackId) &&
                    Objects.equals(clientId, that.clientId) &&
                    Objects.equals(clientAddress, that.clientAddress) &&
                    Objects.equals(principal, that.principal) &&
                    Objects.equals(listenerName, that.listenerName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rackId, clientId, clientAddress, principal, listenerName);
        }

        @Override
        public String toString() {
            return "DefaultClientMetadata{" +
                    "rackId='" + rackId + '\'' +
                    ", clientId='" + clientId + '\'' +
                    ", clientAddress=" + clientAddress +
                    ", principal=" + principal +
                    ", listenerName='" + listenerName + '\'' +
                    '}';
        }
    }
}
