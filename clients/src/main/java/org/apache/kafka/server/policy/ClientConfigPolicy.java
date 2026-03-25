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
package org.apache.kafka.server.policy;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.network.ClientInformation;

import java.util.Map;
import java.util.Set;

/**
 * An interface for enforcing client configuration policies.
 *
 * <p>Common use cases are verifying that client configurations match expected values
 * or fall within acceptable ranges for a given client profile (name, version, role).
 *
 * <p>If <code>client.config.policy.class.name</code> is defined, Kafka will create an instance of the specified class
 * using the default constructor and will then pass the broker configs to its <code>configure()</code> method.
 * During broker shutdown, the <code>close()</code> method will be invoked so that resources can be released (if
 * necessary).
 */
@InterfaceStability.Evolving
public interface ClientConfigPolicy extends Configurable, AutoCloseable {

    /**
     * Metadata provided for GetConfigSubscription requests.
     */
    class GetConfigSubscriptionRequestMetadata {
        private final ClientInformation clientInformation;

        public GetConfigSubscriptionRequestMetadata(ClientInformation clientInformation) {
            this.clientInformation = clientInformation;
        }

        /**
         * Return the client information from the ApiVersionsRequest.
         */
        public ClientInformation clientInformation() {
            return clientInformation;
        }
    }

    /**
     * Metadata provided for PushConfig requests.
     */
    class PushConfigRequestMetadata {
        private final ClientInformation clientInformation;
        private final Map<String, String> configs;

        public PushConfigRequestMetadata(ClientInformation clientInformation,
                                        Map<String, String> configs) {
            this.clientInformation = clientInformation;
            this.configs = configs;
        }

        /**
         * Return the client information from the ApiVersionsRequest.
         */
        public ClientInformation clientInformation() {
            return clientInformation;
        }

        /**
         * Return the configuration key-value pairs being pushed by the client.
         */
        public Map<String, String> configs() {
            return configs;
        }
    }

    /**
     * Select which configuration keys the client should send in a subsequent PushConfig request.
     *
     * @param metadata the GetConfigSubscription request metadata
     * @return the set of configuration keys to request, or null/empty set if no configs should be requested
     */
    Set<String> configKeysToRequest(GetConfigSubscriptionRequestMetadata metadata);

    /**
     * Validate the pushed client configurations.
     *
     * @param metadata the PushConfig request metadata
     * @throws PolicyViolationException if the configurations violate the policy
     */
    void validate(PushConfigRequestMetadata metadata) throws PolicyViolationException;

    /**
     * Close this policy instance. Default implementation is a no-op.
     */
    @Override
    default void close() throws Exception {
        // Default no-op
    }
}
