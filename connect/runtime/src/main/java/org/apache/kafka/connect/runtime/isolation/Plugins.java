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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Plugins {
    private static final Logger log = LoggerFactory.getLogger(Plugins.class);
    private final DelegatingClassLoader delegatingLoader;

    public Plugins(Map<String, String> props) {
        List<String> pluginLocations = WorkerConfig.pluginLocations(props);
        delegatingLoader = newDelegatingClassLoader(pluginLocations);
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

    private static <T> String pluginNames(Collection<PluginDesc<T>> plugins) {
        return Utils.join(plugins, ", ");
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

    public static ClassLoader compareAndSwapLoaders(ClassLoader loader) {
        ClassLoader current = Thread.currentThread().getContextClassLoader();
        if (!current.equals(loader)) {
            Thread.currentThread().setContextClassLoader(loader);
        }
        return current;
    }

    public ClassLoader currentThreadLoader() {
        return Thread.currentThread().getContextClassLoader();
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

    public DelegatingClassLoader delegatingLoader() {
        return delegatingLoader;
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
            klass = entry.pluginClass();
        }
        return newPlugin(klass);
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

}
