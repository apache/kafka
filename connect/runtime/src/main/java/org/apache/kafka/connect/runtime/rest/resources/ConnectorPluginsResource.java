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
package org.apache.kafka.connect.runtime.rest.resources;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorPluginInfo;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.tools.MockConnector;
import org.apache.kafka.connect.tools.MockSinkConnector;
import org.apache.kafka.connect.tools.MockSourceConnector;
import org.apache.kafka.connect.tools.SchemaSourceConnector;
import org.apache.kafka.connect.tools.VerifiableSinkConnector;
import org.apache.kafka.connect.tools.VerifiableSourceConnector;
import org.apache.kafka.connect.util.FutureCallback;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Path("/connector-plugins")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ConnectorPluginsResource {

    private static final String ALIAS_SUFFIX = "Connector";
    private final Herder herder;
    private final List<ConnectorPluginInfo> connectorPlugins;

    private static final List<Class<? extends Connector>> CONNECTOR_EXCLUDES = Arrays.asList(
            VerifiableSourceConnector.class, VerifiableSinkConnector.class,
            MockConnector.class, MockSourceConnector.class, MockSinkConnector.class,
            SchemaSourceConnector.class
    );

    public ConnectorPluginsResource(Herder herder) {
        this.herder = herder;
        this.connectorPlugins = new ArrayList<>();
    }

    @PUT
    @Path("/{connectorType}/config/validate")
    public ConfigInfos validateConfigs(
        final @PathParam("connectorType") String connType,
        final Map<String, String> connectorConfig
    ) throws Throwable {
        String includedConnType = connectorConfig.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
        if (includedConnType != null
            && !normalizedPluginName(includedConnType).endsWith(normalizedPluginName(connType))) {
            throw new BadRequestException(
                "Included connector type " + includedConnType + " does not match request type "
                    + connType
            );
        }

        // the validated configs don't need to be logged
        FutureCallback<ConfigInfos> validationCallback = new FutureCallback<>();
        herder.validateConnectorConfig(connectorConfig, validationCallback, false);

        try {
            return validationCallback.get(ConnectorsResource.REQUEST_TIMEOUT_MS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            // This timeout is for the operation itself. None of the timeout error codes are relevant, so internal server
            // error is the best option
            throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), "Request timed out");
        } catch (InterruptedException e) {
            throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), "Request interrupted");
        }
    }

    @GET
    @Path("/")
    public List<ConnectorPluginInfo> listConnectorPlugins() {
        return getConnectorPlugins();
    }

    // TODO: improve once plugins are allowed to be added/removed during runtime.
    private synchronized List<ConnectorPluginInfo> getConnectorPlugins() {
        if (connectorPlugins.isEmpty()) {
            for (PluginDesc<Connector> plugin : herder.plugins().connectors()) {
                if (!CONNECTOR_EXCLUDES.contains(plugin.pluginClass())) {
                    connectorPlugins.add(new ConnectorPluginInfo(plugin));
                }
            }
        }

        return Collections.unmodifiableList(connectorPlugins);
    }

    private String normalizedPluginName(String pluginName) {
        // Works for both full and simple class names. In the latter case, it generates the alias.
        return pluginName.endsWith(ALIAS_SUFFIX) && pluginName.length() > ALIAS_SUFFIX.length()
            ? pluginName.substring(0, pluginName.length() - ALIAS_SUFFIX.length())
            : pluginName;
    }
}
