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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Modules {
    private static final Logger log = LoggerFactory.getLogger(Modules.class);

    private final List<String> moduleTopPaths;
    private final Map<ModuleClassLoader, URL[]> loaders;
    private final DelegatingClassLoader delegatingLoader;

    public Modules(WorkerConfig workerConfig) {
        loaders = new HashMap<>();
        List<String> paths = workerConfig.getList(WorkerConfig.MODULE_PATH_CONFIG);
        moduleTopPaths = paths == null ? new ArrayList<String>() : paths;
        delegatingLoader = new DelegatingClassLoader(moduleTopPaths);
    }

    public void init() {
        delegatingLoader.initLoaders();
    }

    public DelegatingClassLoader getDelegatingLoader() {
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
        return null;
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
}
