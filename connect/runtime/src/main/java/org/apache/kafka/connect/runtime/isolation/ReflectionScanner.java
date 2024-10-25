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

import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.ServiceLoader;
import java.util.SortedSet;
import java.util.TreeSet;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;

/**
 * A {@link PluginScanner} implementation which uses reflection and {@link ServiceLoader} to discover plugins.
 * <p>This implements the legacy discovery strategy, which uses a combination of reflection and service loading in
 * order to discover plugins. Specifically, a plugin appears in the scan result if all the following conditions are true:
 * <ul>
 *     <li>The class and direct dependencies can be loaded</li>
 *     <li>The class is concrete</li>
 *     <li>The class is public</li>
 *     <li>The class has a no-args constructor</li>
 *     <li>The no-args constructor is public</li>
 *     <li>Static initialization of the class completes without throwing an exception</li>
 *     <li>The no-args constructor completes without throwing an exception</li>
 *     <li>One of the following is true:
 *         <ul>
 *             <li>Is a subclass of {@link SinkConnector}, {@link SourceConnector}, {@link Converter},
 *             {@link HeaderConverter}, {@link Transformation}, or {@link Predicate}</li>
 *             <li>Is a subclass of {@link ConfigProvider}, {@link ConnectRestExtension}, or
 *             {@link ConnectorClientConfigOverridePolicy}, and has a {@link ServiceLoader} compatible
 *             manifest file or module declaration</li>
 *         </ul>
 *     </li>
 * </ul>
 * <p>Note: This scanner has a runtime proportional to the number of overall classes in the passed-in
 * {@link PluginSource} objects, which may be significant for plugins with large dependencies. For a more performant
 * implementation, consider using {@link ServiceLoaderScanner} and follow migration instructions for
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-898%3A+Modernize+Connect+plugin+discovery">KIP-898</a>.
 */
public class ReflectionScanner extends PluginScanner {

    private static final Logger log = LoggerFactory.getLogger(ReflectionScanner.class);

    private static <T> String versionFor(Class<? extends T> pluginKlass) throws ReflectiveOperationException {
        T pluginImpl = pluginKlass.getDeclaredConstructor().newInstance();
        return versionFor(pluginImpl);
    }

    @Override
    protected PluginScanResult scanPlugins(PluginSource source) {
        ClassGraph classGraphBuilder = new ClassGraph()
                .addClassLoader(source.loader())
                .enableExternalClasses()
                .enableClassInfo();
        try (ScanResult classGraph = classGraphBuilder.scan()) {
            return new PluginScanResult(
                  getPluginDesc(classGraph, PluginType.SINK, source),
                  getPluginDesc(classGraph, PluginType.SOURCE, source),
                  getPluginDesc(classGraph, PluginType.CONVERTER, source),
                  getPluginDesc(classGraph, PluginType.HEADER_CONVERTER, source),
                  getPluginDesc(classGraph, PluginType.ERROR_RECORD_REPORTER, source),
                  getTransformationPluginDesc(source, classGraph),
                  getPredicatePluginDesc(source, classGraph),
                  getServiceLoaderPluginDesc(PluginType.CONFIGPROVIDER, source),
                  getServiceLoaderPluginDesc(PluginType.REST_EXTENSION, source),
                  getServiceLoaderPluginDesc(PluginType.CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY, source)
          );
        }
    }

    @SuppressWarnings({"unchecked"})
    private SortedSet<PluginDesc<Predicate<?>>> getPredicatePluginDesc(PluginSource source, ScanResult classGraph) {
        return (SortedSet<PluginDesc<Predicate<?>>>) (SortedSet<?>) getPluginDesc(classGraph, PluginType.PREDICATE, source);
    }

    @SuppressWarnings({"unchecked"})
    private SortedSet<PluginDesc<Transformation<?>>> getTransformationPluginDesc(PluginSource source, ScanResult classGraph) {
        return (SortedSet<PluginDesc<Transformation<?>>>) (SortedSet<?>) getPluginDesc(classGraph, PluginType.TRANSFORMATION, source);
    }

    @SuppressWarnings({"unchecked"})
    private <T> SortedSet<PluginDesc<T>> getPluginDesc(
            ScanResult classGraph,
            PluginType type,
            PluginSource source
    ) {
        ClassInfoList plugins;
        Class<T> klass = (Class<T>) type.superClass();
        try {
            if (klass.isInterface()) {
                plugins = classGraph.getClassesImplementing(klass.getName());
            } else {
                plugins = classGraph.getSubclasses(klass.getName());
            }
        } catch (Exception e) {
            log.debug("Reflections scanner could not find any {} in {} for URLs: {}",
                    type, source, source.urls(), e);
            return Collections.emptySortedSet();
        }

        SortedSet<PluginDesc<T>> result = new TreeSet<>();
        for (Class<? extends T> pluginKlass : plugins.getStandardClasses().loadClasses(klass, true)) {
            if (!PluginUtils.isConcrete(pluginKlass)) {
                log.debug("Skipping {} in {} as it is not concrete implementation", pluginKlass, source);
                continue;
            }
            if (pluginKlass.getClassLoader() != source.loader()) {
                log.debug("{} from other classloader {} is visible from {}, excluding to prevent isolated loading",
                        pluginKlass, pluginKlass.getClassLoader(), source);
                continue;
            }
            try (LoaderSwap loaderSwap = withClassLoader(source.loader())) {
                result.add(pluginDesc(pluginKlass, versionFor(pluginKlass), type, source));
            } catch (ReflectiveOperationException | LinkageError e) {
                log.error("Failed to discover {} in {}: Unable to instantiate {}{}",
                        type.simpleName(), source, pluginKlass.getSimpleName(),
                        reflectiveErrorDescription(e), e);
            }
        }
        return result;
    }
}
