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
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * <p>An interface for enforcing a policy on alter configs requests.
 *
 * <p>Common use cases are requiring that the replication factor, <code>min.insync.replicas</code> and/or retention settings for a
 * topic remain within an allowable range.
 *
 * <p>If <code>alter.config.policy.class.name</code> is defined, Kafka will create an instance of the specified class
 * using the default constructor and will then pass the broker configs to its <code>configure()</code> method. During
 * broker shutdown, the <code>close()</code> method will be invoked so that resources can be released (if necessary).
 */
public interface AlterConfigPolicy extends Configurable, AutoCloseable {

    /**
     * Class containing the create request parameters.
     */
    class RequestMetadata {

        private final ConfigResource resource;
        private final Map<String, String> configs;

        private final KafkaPrincipal principal;

        /**
         * Create an instance of this class with the provided parameters.
         *
         * This constructor is public to make testing of <code>AlterConfigPolicy</code> implementations easier.
         */
        public RequestMetadata(ConfigResource resource, Map<String, String> configs, KafkaPrincipal principal) {
            this.resource = resource;
            this.configs = configs;
            this.principal = principal;
        }

        /**
         * Return the configs in the request.
         */
        public Map<String, String> configs() {
            return configs;
        }

        public ConfigResource resource() {
            return resource;
        }

        public KafkaPrincipal principal() {
            return principal;
        }

        @Override
        public int hashCode() {
            return Objects.hash(resource, configs);
        }

        @Override
        public boolean equals(Object o) {
            if ((o == null) || (!o.getClass().equals(getClass()))) return false;
            RequestMetadata other = (RequestMetadata) o;
            return resource.equals(other.resource) &&
                configs.equals(other.configs);
        }

        @Override
        public String toString() {
            return "AlterConfigPolicy.RequestMetadata(resource=" + resource +
                    ", configs=" + configs + ")";
        }
    }

    /**
     * Validate the request parameters and throw a <code>PolicyViolationException</code> with a suitable error
     * message if the alter configs request parameters for the provided resource do not satisfy this policy.
     *
     * Clients will receive the POLICY_VIOLATION error code along with the exception's message. Note that validation
     * failure only affects the relevant resource, other resources in the request will still be processed.
     *
     * @param requestMetadata the alter configs request parameters for the provided resource (topic is the only resource
     *                        type whose configs can be updated currently).
     * @throws PolicyViolationException if the request parameters do not satisfy this policy.
     */
    void validate(RequestMetadata requestMetadata) throws PolicyViolationException;
}
