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
package org.apache.kafka.connect.runtime.isolation;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Plugins {
    private static final Logger log = LoggerFactory.getLogger(Plugins.class);

    private final List<String> pluginTopPaths;
    private final Map<PluginClassLoader, URL[]> loaders;
    private final DelegatingClassLoader delegatingLoader;

    public Plugins(Map<String, String> props) {
        loaders = new HashMap<>();
        String pathList = props.get(WorkerConfig.PLUGIN_PATH_CONFIG);
        pluginTopPaths = pathList == null
                         ? new ArrayList<String>()
                         : Arrays.asList(pathList.trim().split("\\s*,\\s*", -1));
        delegatingLoader = newDelegatingClassLoader(pluginTopPaths);
        delegatingLoader.initLoaders();
    }

    private static DelegatingClassLoader newDelegatingClassLoader(final List<String> paths) {
        return (DelegatingClassLoader) AccessController.doPrivileged(
                new PrivilegedAction() {
                    @Override
                    public Object run() {
                        return new DelegatingClassLoader(paths);
                    }
                }
        );
    }

    public DelegatingClassLoader delegatingLoader() {
        return delegatingLoader;
    }

    public Map<PluginClassLoader, URL[]> getLoaders() {
        return loaders;
    }

    public Set<PluginDesc<Connector>> connectors() {
        return delegatingLoader.connectors();
    }

    public Set<PluginDesc<Converter>> converters() {
        return delegatingLoader.converters();
    }

    public Set<PluginDesc<Transformation>> transformations() {
        return delegatingLoader.transformations();
    }

    @SuppressWarnings("unchecked")
    public Connector newConnector(String connectorClassOrAlias) {
        Class<? extends Connector> klass;
        try {
            klass = pluginClass(
                    delegatingLoader,
                    connectorClassOrAlias,
                    Connector.class
            );
        } catch (ClassNotFoundException e) {
            List<PluginDesc<Connector>> matches = new ArrayList<>();
            for (PluginDesc<Connector> plugin : delegatingLoader.connectors()) {
                Class<?> pluginClass = plugin.pluginClass();
                String simpleName = pluginClass.getSimpleName();
                if (simpleName.equals(connectorClassOrAlias)
                        || simpleName.equals(connectorClassOrAlias + "Connector")) {
                    matches.add(plugin);
                }
            }

            if (matches.isEmpty()) {
                throw new ConnectException(
                        "Failed to find any class that implements Connector and which name matches "
                        + connectorClassOrAlias
                        + ", available connectors are: "
                        + pluginNames(delegatingLoader.connectors())
                );
            }
            if (matches.size() > 1) {
                throw new ConnectException(
                        "More than one connector matches alias "
                        + connectorClassOrAlias
                        +
                        ". Please use full package and class name instead. Classes found: "
                        + pluginNames(matches)
                );
            }

            PluginDesc<Connector> entry = matches.get(0);
            delegatingLoader.addAlias(entry, connectorClassOrAlias);
            klass = entry.pluginClass();
        }
        return newPlugin(klass);
    }

    private static <T> String pluginNames(Collection<PluginDesc<T>> plugins) {
        StringBuilder names = new StringBuilder();
        for (PluginDesc<T> plugin : plugins) {
            names.append(plugin.className()).append(", ");
        }
        String result = names.toString();
        return result.isEmpty()
               ? ""
               : result.substring(0, result.length() - 2);
    }

    public Task newTask(Class<? extends Task> taskClass) {
        return newPlugin(taskClass);
    }

    public Converter newConverter(String converterClassOrAlias) {
        return newConverter(converterClassOrAlias, null);
    }

    public Converter newConverter(String converterClassOrAlias, AbstractConfig config) {
        Class<? extends Converter> klass;
        try {
            klass = pluginClass(
                    delegatingLoader,
                    converterClassOrAlias,
                    Converter.class
            );
        } catch (ClassNotFoundException e) {
            throw new ConnectException(
                    "Failed to find any class that implements Converter and which name matches "
                            + converterClassOrAlias
                            + ", available connectors are: "
                            + pluginNames(delegatingLoader.converters())
            );
        }
        return config != null ? newConfiguredPlugin(config, klass) : newPlugin(klass);
    }

    public <R extends ConnectRecord<R>> Transformation<R> newTranformations(
            String transformationClassOrAlias
    ) {
        return null;
    }

    protected static <T> T newPlugin(Class<T> klass) {
        try {
            return Utils.newInstance(klass);
        } catch (Throwable t) {
            throw new ConnectException("Instantiation error", t);
        }
    }

    protected static <T> T newConfiguredPlugin(AbstractConfig config, Class<T> klass) {
        T plugin = Utils.newInstance(klass);
        if (plugin instanceof Configurable) {
            ((Configurable) plugin).configure(config.originals());
        }
        return plugin;
    }

    @SuppressWarnings("unchecked")
    protected static <U> Class<? extends U> pluginClass(
            DelegatingClassLoader loader,
            String classOrAlias,
            Class<U> pluginClass
    ) throws ClassNotFoundException {
        Class<?> klass = loader.loadClass(classOrAlias, false);
        if (pluginClass.isAssignableFrom(klass)) {
            return (Class<? extends U>) klass;
        }

        throw new ClassNotFoundException(
                "Requested class: "
                + classOrAlias
                + " does not extend " + pluginClass.getSimpleName()
        );
    }

    public ClassLoader compareAndSwapWithDelegatingLoader() {
        ClassLoader current = Thread.currentThread().getContextClassLoader();
        if (!current.equals(delegatingLoader)) {
            Thread.currentThread().setContextClassLoader(delegatingLoader);
        }
        return current;
    }

    public ClassLoader compareAndSwapLoaders(Connector connector) {
        ClassLoader connectorLoader = delegatingLoader.connectorLoader(connector);
        return compareAndSwapLoaders(connectorLoader);
    }

    public static final ClassLoader compareAndSwapLoaders(ClassLoader loader) {
        ClassLoader current = Thread.currentThread().getContextClassLoader();
        if (!current.equals(loader)) {
            Thread.currentThread().setContextClassLoader(loader);
        }
        return current;
    }

}
