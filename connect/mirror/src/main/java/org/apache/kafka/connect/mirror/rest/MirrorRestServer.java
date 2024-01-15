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
package org.apache.kafka.connect.mirror.rest;

import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.mirror.rest.resources.InternalMirrorResource;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.RestServerConfig;
import org.apache.kafka.connect.runtime.rest.resources.ConnectResource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class MirrorRestServer extends RestServer {

    private final RestClient restClient;
    private Map<SourceAndTarget, Herder> herders;

    public MirrorRestServer(Map<?, ?> props, RestClient restClient) {
        super(RestServerConfig.forInternal(props));
        this.restClient = restClient;
    }

    public void initializeInternalResources(Map<SourceAndTarget, Herder> herders) {
        this.herders = herders;
        super.initializeResources();
    }

    @Override
    protected Collection<ConnectResource> regularResources() {
        return Arrays.asList(
                new InternalMirrorResource(herders, restClient)
        );
    }

    @Override
    protected Collection<ConnectResource> adminResources() {
        return Collections.emptyList();
    }

}
