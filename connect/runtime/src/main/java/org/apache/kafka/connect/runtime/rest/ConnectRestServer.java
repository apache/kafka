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

import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.resources.ConnectResource;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorPluginsResource;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource;
import org.apache.kafka.connect.runtime.rest.resources.InternalConnectResource;
import org.apache.kafka.connect.runtime.rest.resources.LoggingResource;
import org.apache.kafka.connect.runtime.rest.resources.RootResource;
import org.glassfish.jersey.server.ResourceConfig;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class ConnectRestServer extends RestServer {

    private final RestClient restClient;
    private Herder herder;

    public ConnectRestServer(Integer rebalanceTimeoutMs, RestClient restClient, Map<?, ?> props) {
        super(RestServerConfig.forPublic(rebalanceTimeoutMs, props));
        this.restClient = restClient;
    }

    public void initializeResources(Herder herder) {
        this.herder = herder;
        super.initializeResources();
    }

    @Override
    protected Collection<ConnectResource> regularResources() {
        return Arrays.asList(
                new RootResource(herder),
                new ConnectorsResource(herder, config, restClient),
                new InternalConnectResource(herder, restClient),
                new ConnectorPluginsResource(herder)
        );
    }

    @Override
    protected Collection<ConnectResource> adminResources() {
        return Arrays.asList(
                new LoggingResource(herder)
        );
    }

    @Override
    protected void configureRegularResources(ResourceConfig resourceConfig) {
        registerRestExtensions(herder, resourceConfig);
    }

}
