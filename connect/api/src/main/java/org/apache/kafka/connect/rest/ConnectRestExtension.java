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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.health.ConnectClusterState;

import java.io.Closeable;
import java.util.Map;

/**
 * A plugin interface to allow registration of new JAX-RS resources like Filters, REST endpoints,
 * providers, etc. The implementations will be discovered by the standard Java {@link
 * java.util.ServiceLoader} mechanism by  Connect's plugin class loading mechanism.
 * The implementation class must be packaged in a JAR that includes the
 * {@code META-INF/services/org.apache.kafka.connect.rest.extension.ConnectRestExtension} containing
 * the fully qualified name of the implementation class.
 *
 * <p> When Connect's worker configuration uses the REST extension implementation class,
 * upon startup Connect will instantiate the implementation and pass the configuration
 * to the instance via {@link Configurable#configure(Map)}.
 *
 * <p> Typical use cases that can be implemented using this interface include things like security
 * (authentication and authorization), logging, request validations, etc.
 */
public interface ConnectRestExtension extends Configurable, Versioned, Closeable {

    /**
     * ConnectRestExtension implementation can register custom JAX-RS resources via the {@link
     * #register(ConnectRestExtensionContext)} method. The Connect Framework will invoke this method
     * after registering the default Connect resources. If the implementations attempt to
     * re-register any of the Connect resources, it will be be ignored and will be logged.
     *
     * @param restPluginContext The context provides access to JAX-RS {@link javax.ws.rs.core.Configurable}
     *                          and {@link ConnectClusterState}.The custom JAX-RS resources can be
     *                          registered via the {@link ConnectRestExtensionContext#configurable()}
     */
    void register(ConnectRestExtensionContext restPluginContext);
}
