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

package org.apache.kafka.connect.components;

import org.apache.kafka.common.config.ConfigDef;

/**
 * Interface for components that provide version and configuration specifications.
 * This interface establishes a common contract for all Kafka Connect components
 * that define a version and expose configurable properties, enabling uniform discovery and introspection
 * of component configurations.
 *
 * <p>Components implementing this interface declare their version and configuration requirements
 * through a {@link ConfigDef} object, which describes the configuration properties
 * including their names, types, default values, validators, and documentation.
 *
 */
public interface ConnectPlugin extends Versioned {

    /**
     * Returns the configuration specification for this component.
     *
     * <p>The returned {@link ConfigDef} object describes all configuration properties
     * that this component accepts, including their types, default values, validators,
     * importance levels, and documentation strings.
     *
     * @return the configuration definition for this component; never null
     */
    ConfigDef config();
}
