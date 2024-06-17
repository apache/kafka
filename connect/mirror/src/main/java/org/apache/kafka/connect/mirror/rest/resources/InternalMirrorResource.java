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
package org.apache.kafka.connect.mirror.rest.resources;

import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestRequestTimeout;
import org.apache.kafka.connect.runtime.rest.resources.InternalClusterResource;

import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import java.util.Map;

@Path("/{source}/{target}/connectors")
public class InternalMirrorResource extends InternalClusterResource {

    @Context
    private UriInfo uriInfo;

    private final Map<SourceAndTarget, Herder> herders;

    @Inject
    public InternalMirrorResource(
            Map<SourceAndTarget, Herder> herders,
            RestClient restClient,
            RestRequestTimeout requestTimeout
    ) {
        super(restClient, requestTimeout);
        this.herders = herders;
    }

    @Override
    protected Herder herderForRequest() {
        String source = pathParam("source");
        String target = pathParam("target");
        Herder result = herders.get(new SourceAndTarget(source, target));
        if (result == null) {
            throw new NotFoundException("No replication flow found for source '" + source + "' and target '" + target + "'");
        }
        return result;
    }

    private String pathParam(String name) {
        String result = uriInfo.getPathParameters().getFirst(name);
        if (result == null)
            throw new NotFoundException("Could not parse " + name + " cluster from request path");
        return result;
    }

}
