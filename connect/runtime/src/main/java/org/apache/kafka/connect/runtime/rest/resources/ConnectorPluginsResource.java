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
import org.apache.kafka.connect.runtime.PredicatedTransformation;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.PluginType;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigKeyInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorPluginInfo;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.tools.MockConnector;
import org.apache.kafka.connect.tools.MockSinkConnector;
import org.apache.kafka.connect.tools.MockSourceConnector;
import org.apache.kafka.connect.tools.SchemaSourceConnector;
import org.apache.kafka.connect.tools.VerifiableSinkConnector;
import org.apache.kafka.connect.tools.VerifiableSourceConnector;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.util.FutureCallback;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Path("/connector-plugins")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ConnectorPluginsResource {

    private static final String ALIAS_SUFFIX = "Connector";
    private final Herder herder;
    private final List<ConnectorPluginInfo> connectorPlugins;
    private final Map<String, PluginType> pluginsByType;

    static final List<Class<? extends Connector>> CONNECTOR_EXCLUDES = Arrays.asList(
            VerifiableSourceConnector.class, VerifiableSinkConnector.class,
            MockConnector.class, MockSourceConnector.class, MockSinkConnector.class,
            SchemaSourceConnector.class
    );

    @SuppressWarnings("rawtypes")
    static final List<Class<? extends Transformation>> TRANSFORM_EXCLUDES = Arrays.asList(
            PredicatedTransformation.class
    );

    public ConnectorPluginsResource(Herder herder) {
        this.herder = herder;
        this.connectorPlugins = new ArrayList<>();
        this.pluginsByType = new HashMap<>();

        // TODO: improve once plugins are allowed to be added/removed during runtime.
        for (PluginDesc<Connector> plugin : herder.plugins().connectors()) {
            if (!CONNECTOR_EXCLUDES.contains(plugin.pluginClass())) {
                connectorPlugins.add(new ConnectorPluginInfo(plugin, PluginType.from(plugin.pluginClass())));
                pluginsByType.put(getAlias(plugin.className()), PluginType.from(plugin.pluginClass()));
            }
        }
        for (PluginDesc<Transformation<?>> transform : herder.plugins().transformations()) {
            if (!TRANSFORM_EXCLUDES.contains(transform.pluginClass())) {
                connectorPlugins.add(new ConnectorPluginInfo(transform, PluginType.TRANSFORMATION));
                pluginsByType.put(getAlias(transform.className()), PluginType.TRANSFORMATION);
            }
        }
        for (PluginDesc<Predicate<?>> predicate : herder.plugins().predicates()) {
            connectorPlugins.add(new ConnectorPluginInfo(predicate, PluginType.PREDICATE));
            pluginsByType.put(getAlias(predicate.className()), PluginType.PREDICATE);
        }
        for (PluginDesc<Converter> converter : herder.plugins().converters()) {
            connectorPlugins.add(new ConnectorPluginInfo(converter, PluginType.CONVERTER));
            pluginsByType.put(getAlias(converter.className()), PluginType.CONVERTER);
        }
        for (PluginDesc<HeaderConverter> headerConverter : herder.plugins().headerConverters()) {
            connectorPlugins.add(new ConnectorPluginInfo(headerConverter, PluginType.HEADER_CONVERTER));
            pluginsByType.put(getAlias(headerConverter.className()), PluginType.HEADER_CONVERTER);
        }
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
            return validationCallback.get(ConnectorsResource.REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
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
    public List<ConnectorPluginInfo> listConnectorPlugins(@DefaultValue("true") @QueryParam("connectorsOnly") boolean connectorsOnly) {
        return getConnectorPlugins(connectorsOnly);
    }

    @GET
    @Path("/{type}/config")
    public List<ConfigKeyInfo> getConnectorConfigDef(final @PathParam("type") String type) {
        return doGetConfigDef(type);
    }

    private synchronized List<ConnectorPluginInfo> getConnectorPlugins(boolean connectorsOnly) {
        if (connectorsOnly) {
            Set<String> types = new HashSet<>(Arrays.asList(PluginType.SINK.toString(), PluginType.SOURCE.toString()));
            return Collections.unmodifiableList(connectorPlugins.stream().filter(p -> types.contains(p.type())).collect(Collectors.toList()));
        } else {
            return Collections.unmodifiableList(connectorPlugins);
        }
    }

    private String normalizedPluginName(String pluginName) {
        // Works for both full and simple class names. In the latter case, it generates the alias.
        return pluginName.endsWith(ALIAS_SUFFIX) && pluginName.length() > ALIAS_SUFFIX.length()
            ? pluginName.substring(0, pluginName.length() - ALIAS_SUFFIX.length())
            : pluginName;
    }

    String getAlias(String name) {
        name = normalizedPluginName(name);
        int lastIndexOf = name.lastIndexOf('.');
        return lastIndexOf >= 0 ? name.substring(lastIndexOf + 1) : name;
    }

    private synchronized List<ConfigKeyInfo> doGetConfigDef(final String pluginName) {
        PluginType pluginType = pluginsByType.getOrDefault(getAlias(pluginName), PluginType.UNKNOWN);
        return herder.connectorPluginConfig(pluginType, pluginName);
    }
}
