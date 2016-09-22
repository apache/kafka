/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class ConnectorFactory {

    public Connector newConnector(String connectorClassOrAlias) {
        return instantiate(getConnectorClass(connectorClassOrAlias));
    }

    public Task newTask(Class<? extends Task> taskClass) {
        return instantiate(taskClass);
    }

    private static <T> T instantiate(Class<? extends T> cls) {
        try {
            return Utils.newInstance(cls);
        } catch (Throwable t) {
            throw new ConnectException("Instantiation error", t);
        }
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends Connector> getConnectorClass(String connectorClassOrAlias) {
        // Avoid the classpath scan if the full class name was provided
        try {
            Class<?> clazz = Class.forName(connectorClassOrAlias);
            if (!Connector.class.isAssignableFrom(clazz))
                throw new ConnectException("Class " + connectorClassOrAlias + " does not implement Connector");
            return (Class<? extends Connector>) clazz;
        } catch (ClassNotFoundException e) {
            // Fall through to scan for the alias
        }

        // Iterate over our entire classpath to find all the connectors and hopefully one of them matches the alias from the connector configration
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forJavaClassPath()));

        Set<Class<? extends Connector>> connectors = reflections.getSubTypesOf(Connector.class);

        List<Class<? extends Connector>> results = new ArrayList<>();

        for (Class<? extends Connector> connector: connectors) {
            // Configuration included the class name but not package
            if (connector.getSimpleName().equals(connectorClassOrAlias))
                results.add(connector);

            // Configuration included a short version of the name (i.e. FileStreamSink instead of FileStreamSinkConnector)
            if (connector.getSimpleName().equals(connectorClassOrAlias + "Connector"))
                results.add(connector);
        }

        if (results.isEmpty())
            throw new ConnectException("Failed to find any class that implements Connector and which name matches " + connectorClassOrAlias +
                    ", available connectors are: " + connectorNames(connectors));
        if (results.size() > 1) {
            throw new ConnectException("More than one connector matches alias " +  connectorClassOrAlias +
                    ". Please use full package and class name instead. Classes found: " + connectorNames(results));
        }

        // We just validated that we have exactly one result, so this is safe
        return results.get(0);
    }

    private static String connectorNames(Collection<Class<? extends Connector>> connectors) {
        StringBuilder names = new StringBuilder();
        for (Class<?> c : connectors)
            names.append(c.getName()).append(", ");
        return names.substring(0, names.toString().length() - 2);
    }

}
