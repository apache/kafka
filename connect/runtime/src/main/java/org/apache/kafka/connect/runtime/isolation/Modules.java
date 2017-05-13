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

public class Modules {
    private static final Logger log = LoggerFactory.getLogger(Modules.class);

    private final List<String> moduleTopPaths;
    private final Map<ModuleClassLoader, URL[]> loaders;
    private final DelegatingClassLoader delegatingLoader;

    public Modules(Map<String, String> props) {
        loaders = new HashMap<>();
        String pathList = props.get(WorkerConfig.MODULE_PATH_CONFIG);
        moduleTopPaths = pathList == null
                         ? new ArrayList<String>()
                         : Arrays.asList(pathList.trim().split("\\s*,\\s*", -1));
        delegatingLoader = newDelegatingClassLoader(moduleTopPaths);
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

    public Map<ModuleClassLoader, URL[]> getLoaders() {
        return loaders;
    }

    @SuppressWarnings("unchecked")
    public Connector newConnector(String connectorClassOrAlias) {
        Class<? extends Connector> klass;
        try {
            klass = moduleClass(
                    delegatingLoader,
                    connectorClassOrAlias,
                    Connector.class
            );
        } catch (ClassNotFoundException e) {
            List<ModuleDesc<Connector>> matches = new ArrayList<>();
            for (ModuleDesc<Connector> module : delegatingLoader.connectors()) {
                Class<?> moduleClass = module.moduleClass();
                String simpleName = moduleClass.getSimpleName();
                if (simpleName.equals(connectorClassOrAlias)
                        || simpleName.equals(connectorClassOrAlias + "Connector")) {
                    matches.add(module);
                }
            }

            if (matches.isEmpty()) {
                throw new ConnectException(
                        "Failed to find any class that implements Connector and which name matches "
                        + connectorClassOrAlias
                        + ", available connectors are: "
                        + moduleNames(delegatingLoader.connectors())
                );
            }
            if (matches.size() > 1) {
                throw new ConnectException(
                        "More than one connector matches alias "
                        + connectorClassOrAlias
                        +
                        ". Please use full package and class name instead. Classes found: "
                        + moduleNames(matches)
                );
            }

            ModuleDesc<Connector> entry = matches.get(0);
            delegatingLoader.addAlias(entry, connectorClassOrAlias);
            klass = entry.moduleClass();
        }
        return newModule(klass);
    }

    private static <T> String moduleNames(Collection<ModuleDesc<T>> modules) {
        StringBuilder names = new StringBuilder();
        for (ModuleDesc<T> module : modules) {
            names.append(module.className()).append(", ");
        }
        String result = names.toString();
        return result.isEmpty()
               ? ""
               : result.substring(0, result.length() - 2);
    }

    public Task newTask(Class<? extends Task> taskClass) {
        return newModule(taskClass);
    }

    public Converter newConverter(String converterClassOrAlias) {
        return newConverter(converterClassOrAlias, null);
    }

    public Converter newConverter(String converterClassOrAlias, AbstractConfig config) {
        Class<? extends Converter> klass;
        try {
            klass = moduleClass(
                    delegatingLoader,
                    converterClassOrAlias,
                    Converter.class
            );
        } catch (ClassNotFoundException e) {
            throw new ConnectException(
                    "Failed to find any class that implements Converter and which name matches "
                            + converterClassOrAlias
                            + ", available connectors are: "
                            + moduleNames(delegatingLoader.converters())
            );
        }
        return config != null ? newConfiguredModule(config, klass) : newModule(klass);
    }

    public <R extends ConnectRecord<R>> Transformation<R> newTranformations(
            String transformationClassOrAlias
    ) {
        return null;
    }

    protected static <T> T newModule(Class<T> klass) {
        try {
            return Utils.newInstance(klass);
        } catch (Throwable t) {
            throw new ConnectException("Instantiation error", t);
        }
    }

    protected static <T> T newConfiguredModule(AbstractConfig config, Class<T> klass) {
        T module = Utils.newInstance(klass);
        if (module instanceof Configurable) {
            ((Configurable) module).configure(config.originals());
        }
        return module;
    }

    @SuppressWarnings("unchecked")
    protected static <U> Class<? extends U> moduleClass(
            DelegatingClassLoader loader,
            String classOrAlias,
            Class<U> moduleClass
    ) throws ClassNotFoundException {
        Class<?> klass = loader.loadClass(classOrAlias, false);
        if (moduleClass.isAssignableFrom(klass)) {
            return (Class<? extends U>) klass;
        }

        throw new ClassNotFoundException(
                "Requested class: "
                + classOrAlias
                + " does not extend " + moduleClass.getSimpleName()
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
