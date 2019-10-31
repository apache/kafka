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
package org.apache.kafka.common.security.auth;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.security.Provider;
import java.util.Map;

/**
 * An interface for generating security providers.
 */
@InterfaceStability.Evolving
public interface SecurityProviderCreator extends Configurable {

    /**
     * Configure method is used to configure the generator to create the Security Provider
     * @param config configuration parameters for initialising security provider
     */
    default void configure(Map<String, ?> config) {

    }

    /**
     * Generate the security provider configured
     */
    Provider getProvider();
}
