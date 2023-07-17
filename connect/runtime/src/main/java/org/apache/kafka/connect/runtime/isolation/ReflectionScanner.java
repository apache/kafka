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
import org.reflections.Reflections;
import org.reflections.ReflectionsException;
import org.reflections.scanners.Scanners;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A {@link PluginScanner} implementation which uses reflection and {@link ServiceLoader} to discover plugins.
 * <p>This implements the legacy discovery strategy, which uses a combination of reflection and service loading in
 * order to discover plugins. Specifically, a plugin appears in the scan result if all the following conditions are true:
 * <ul>
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
 * {@link PluginSource} objects, which may be significant for plugins with large dependencies.
 */
public class ReflectionScanner extends PluginScanner {

    private static final Logger log = LoggerFactory.getLogger(ReflectionScanner.class);

    public static <T> String versionFor(Class<? extends T> pluginKlass) throws ReflectiveOperationException {
        T pluginImpl = pluginKlass.getDeclaredConstructor().newInstance();
        return versionFor(pluginImpl);
    }

    @Override
    protected PluginScanResult scanPlugins(PluginSource source) {
        ClassLoader loader = source.loader();
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.setClassLoaders(new ClassLoader[]{loader});
        builder.addUrls(source.urls());
        builder.setScanners(Scanners.SubTypes);
        builder.setParallel(true);
        Reflections reflections = new Reflections(builder);

        return new PluginScanResult(
                getPluginDesc(reflections, SinkConnector.class, loader),
                getPluginDesc(reflections, SourceConnector.class, loader),
                getPluginDesc(reflections, Converter.class, loader),
                getPluginDesc(reflections, HeaderConverter.class, loader),
                getTransformationPluginDesc(loader, reflections),
                getPredicatePluginDesc(loader, reflections),
                getServiceLoaderPluginDesc(ConfigProvider.class, loader),
                getServiceLoaderPluginDesc(ConnectRestExtension.class, loader),
                getServiceLoaderPluginDesc(ConnectorClientConfigOverridePolicy.class, loader)
        );
    }

    @SuppressWarnings({"unchecked"})
    private SortedSet<PluginDesc<Predicate<?>>> getPredicatePluginDesc(ClassLoader loader, Reflections reflections) {
        return (SortedSet<PluginDesc<Predicate<?>>>) (SortedSet<?>) getPluginDesc(reflections, Predicate.class, loader);
    }

    @SuppressWarnings({"unchecked"})
    private SortedSet<PluginDesc<Transformation<?>>> getTransformationPluginDesc(ClassLoader loader, Reflections reflections) {
        return (SortedSet<PluginDesc<Transformation<?>>>) (SortedSet<?>) getPluginDesc(reflections, Transformation.class, loader);
    }

    private <T> SortedSet<PluginDesc<T>> getPluginDesc(
            Reflections reflections,
            Class<T> klass,
            ClassLoader loader
    ) {
        Set<Class<? extends T>> plugins;
        try {
            plugins = reflections.getSubTypesOf(klass);
        } catch (ReflectionsException e) {
            log.debug("Reflections scanner could not find any classes for URLs: " +
                    reflections.getConfiguration().getUrls(), e);
            return Collections.emptySortedSet();
        }

        SortedSet<PluginDesc<T>> result = new TreeSet<>();
        for (Class<? extends T> pluginKlass : plugins) {
            if (!PluginUtils.isConcrete(pluginKlass)) {
                log.debug("Skipping {} as it is not concrete implementation", pluginKlass);
                continue;
            }
            if (pluginKlass.getClassLoader() != loader) {
                log.debug("{} from other classloader {} is visible from {}, excluding to prevent isolated loading",
                        pluginKlass.getSimpleName(), pluginKlass.getClassLoader(), loader);
                continue;
            }
            try (LoaderSwap loaderSwap = withClassLoader(loader)) {
                result.add(pluginDesc(pluginKlass, versionFor(pluginKlass), loader));
            } catch (ReflectiveOperationException | LinkageError e) {
                log.error("Failed to discover {}: Unable to instantiate {}{}", klass.getSimpleName(), pluginKlass.getSimpleName(), reflectiveErrorDescription(e), e);
            }
        }
        return result;
    }
}
