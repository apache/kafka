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
 * A plugin interface to allow registration of new JAX-RS resources like Filters, REST endpoints, providers, etc. The implementations will
 * be discovered using the standard Java {@link java.util.ServiceLoader} mechanism by  Connect's plugin class loading mechanism.
 *
 * <p>The extension class(es) must be packaged as a plugin, with one JAR containing the implementation classes and a {@code
 * META-INF/services/org.apache.kafka.connect.rest.extension.ConnectRestExtension} file that contains the fully qualified name of the
 * class(es) that implement the ConnectRestExtension interface. The plugin should also include the JARs of all dependencies except those
 * already provided by the Connect framework.
 *
 * <p>To install into a Connect installation, add a directory named for the plugin and containing the plugin's JARs into a directory that is
 * on Connect's {@code plugin.path}, and (re)start the Connect worker.
 *
 * <p>When the Connect worker process starts up, it will read its configuration and instantiate all of the REST extension implementation
 * classes that are specified in the `rest.extension.classes` configuration property. Connect will then pass its configuration to each
 * extension via the {@link Configurable#configure(Map)} method, and will then call {@link #register} with a provided context.
 *
 * <p>When the Connect worker shuts down, it will call the extension's {@link #close} method to allow the implementation to release all of
 * its resources.
 */
public interface ConnectRestExtension extends Configurable, Versioned, Closeable {

    /**
     * ConnectRestExtension implementations can register custom JAX-RS resources via the {@link #register(ConnectRestExtensionContext)}
     * method. The Connect framework will invoke this method after registering the default Connect resources. If the implementations attempt
     * to re-register any of the Connect resources, it will be be ignored and will be logged.
     *
     * @param restPluginContext The context provides access to JAX-RS {@link javax.ws.rs.core.Configurable} and {@link
     *                          ConnectClusterState}.The custom JAX-RS resources can be registered via the {@link
     *                          ConnectRestExtensionContext#configurable()}
     */
    void register(ConnectRestExtensionContext restPluginContext);
}
