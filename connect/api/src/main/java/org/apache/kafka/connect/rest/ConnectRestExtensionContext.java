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

package org.apache.kafka.connect.rest;

import org.apache.kafka.connect.health.ConnectClusterState;

import javax.ws.rs.core.Configurable;

/**
 * The interface provides the ability for {@link ConnectRestExtension} implementations to access the JAX-RS
 * {@link javax.ws.rs.core.Configurable} and cluster state {@link ConnectClusterState}. The implementation for the interface is provided
 * by the Connect framework.
 */
public interface ConnectRestExtensionContext {

    /**
     * Provides an implementation of {@link javax.ws.rs.core.Configurable} that be used to register JAX-RS resources.
     *
     * @return @return the JAX-RS {@link javax.ws.rs.core.Configurable}; never {@code null}
     */
    Configurable<? extends Configurable<?>> configurable();

    /**
     * Provides the cluster state and health information about the connectors and tasks.
     *
     * @return the cluster state information; never {@code null}
     */
    ConnectClusterState clusterState();
}
