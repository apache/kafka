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

package org.apache.kafka.image.node.printer;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.metadata.KafkaConfigSchema;


public interface MetadataNodeRedactionCriteria {
    /**
     * Returns true if SCRAM data should be redacted.
     */
    boolean shouldRedactScram();

    /**
     * Returns true if DelegationToken data should be redacted.
     */
    boolean shouldRedactDelegationToken();

    /**
     * Returns true if a configuration should be redacted.
     *
     * @param type      The configuration type.
     * @param key       The configuration key.
     *
     * @return          True if the configuration should be redacted.
     */
    boolean shouldRedactConfig(ConfigResource.Type type, String key);

    class Strict implements MetadataNodeRedactionCriteria {
        public static final Strict INSTANCE = new Strict();

        @Override
        public boolean shouldRedactScram() {
            return true;
        }

        @Override
        public boolean shouldRedactDelegationToken() {
            return true;
        }

        @Override
        public boolean shouldRedactConfig(ConfigResource.Type type, String key) {
            return true;
        }
    }

    class Normal implements MetadataNodeRedactionCriteria {
        private final KafkaConfigSchema configSchema;

        public Normal(KafkaConfigSchema configSchema) {
            this.configSchema = configSchema;
        }

        @Override
        public boolean shouldRedactScram() {
            return true;
        }

        @Override
        public boolean shouldRedactDelegationToken() {
            return true;
        }

        @Override
        public boolean shouldRedactConfig(ConfigResource.Type type, String key) {
            return configSchema.isSensitive(type, key);
        }
    }

    class Disabled implements MetadataNodeRedactionCriteria {
        public static final Disabled INSTANCE = new Disabled();

        @Override
        public boolean shouldRedactScram() {
            return false;
        }

        @Override
        public boolean shouldRedactDelegationToken() {
            return false;
        }

        @Override
        public boolean shouldRedactConfig(ConfigResource.Type type, String key) {
            return false;
        }
    }
}
