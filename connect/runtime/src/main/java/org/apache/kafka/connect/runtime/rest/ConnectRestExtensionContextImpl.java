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

package org.apache.kafka.connect.runtime.rest;

import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;

import jakarta.ws.rs.core.Configurable;

public class ConnectRestExtensionContextImpl implements ConnectRestExtensionContext {

    private final Configurable<? extends Configurable<?>> configurable;
    private final ConnectClusterState clusterState;

    public ConnectRestExtensionContextImpl(
        Configurable<? extends Configurable<?>> configurable,
        ConnectClusterState clusterState
    ) {
        this.configurable = configurable;
        this.clusterState = clusterState;
    }

    @Override
    public Configurable<? extends Configurable<?>> configurable() {
        return configurable;
    }

    @Override
    public ConnectClusterState clusterState() {
        return clusterState;
    }
}
