/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorPluginInfo;
import org.apache.kafka.connect.tools.MockConnector;
import org.apache.kafka.connect.tools.MockSinkConnector;
import org.apache.kafka.connect.tools.MockSourceConnector;
import org.apache.kafka.connect.tools.SchemaSourceConnector;
import org.apache.kafka.connect.tools.VerifiableSinkConnector;
import org.apache.kafka.connect.tools.VerifiableSourceConnector;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.util.ReflectionsUtil;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class PluginDiscovery {

    private static final List<Class<? extends Connector>> CONNECTOR_EXCLUDES = Arrays.asList(
            VerifiableSourceConnector.class, VerifiableSinkConnector.class,
            MockConnector.class, MockSourceConnector.class, MockSinkConnector.class,
            SchemaSourceConnector.class
    );

    private static final List<Class<? extends Transformation>> TRANSFORMATION_EXCLUDES = Arrays.asList();

    private static boolean scanned = false;
    private static List<ConnectorPluginInfo> validConnectorPlugins;
    private static List<Class<? extends Transformation>> validTransformationPlugins;

    public static synchronized List<ConnectorPluginInfo> connectorPlugins() {
        scanClasspathForPlugins();
        return validConnectorPlugins;
    }

    public static synchronized List<Class<? extends Transformation>> transformationPlugins() {
        scanClasspathForPlugins();
        return validTransformationPlugins;
    }

    public static synchronized void scanClasspathForPlugins() {
        if (scanned) return;
        ReflectionsUtil.registerUrlTypes();
        final Reflections reflections = new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forJavaClassPath()));
        validConnectorPlugins = Collections.unmodifiableList(connectorPlugins(reflections));
        validTransformationPlugins = Collections.unmodifiableList(transformationPlugins(reflections));
        scanned = true;
    }

    private static List<ConnectorPluginInfo> connectorPlugins(Reflections reflections) {
        final Set<Class<? extends Connector>> connectorClasses = reflections.getSubTypesOf(Connector.class);
        connectorClasses.removeAll(CONNECTOR_EXCLUDES);

        final List<ConnectorPluginInfo> connectorPlugins = new ArrayList<>(connectorClasses.size());
        for (Class<? extends Connector> connectorClass : connectorClasses) {
            if (isConcrete(connectorClass)) {
                connectorPlugins.add(new ConnectorPluginInfo(connectorClass.getCanonicalName()));
            }
        }

        Collections.sort(connectorPlugins, new Comparator<ConnectorPluginInfo>() {
            @Override
            public int compare(ConnectorPluginInfo a, ConnectorPluginInfo b) {
                return a.clazz().compareTo(b.clazz());
            }
        });

        return connectorPlugins;
    }

    private static List<Class<? extends Transformation>> transformationPlugins(Reflections reflections) {
        final Set<Class<? extends Transformation>> transformationClasses = reflections.getSubTypesOf(Transformation.class);
        transformationClasses.removeAll(TRANSFORMATION_EXCLUDES);

        final List<Class<? extends Transformation>> transformationPlugins = new ArrayList<>(transformationClasses.size());
        for (Class<? extends Transformation> transformationClass : transformationClasses) {
            if (isConcrete(transformationClass)) {
                transformationPlugins.add(transformationClass);
            }
        }

        Collections.sort(transformationPlugins, new Comparator<Class<? extends Transformation>>() {
            @Override
            public int compare(Class<? extends Transformation> a, Class<? extends Transformation> b) {
                return a.getCanonicalName().compareTo(b.getCanonicalName());
            }
        });

        return transformationPlugins;
    }

    private static boolean isConcrete(Class<?> cls) {
        final int mod = cls.getModifiers();
        return !Modifier.isAbstract(mod) && !Modifier.isInterface(mod);
    }

    public static void main(String... args) {
        System.out.println(connectorPlugins());
        System.out.println(transformationPlugins());
    }

}
