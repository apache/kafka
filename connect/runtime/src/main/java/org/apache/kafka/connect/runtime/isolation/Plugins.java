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
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.reporter.ErrorRecordReporter;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class Plugins {

    public enum ClassLoaderUsage {
        CURRENT_CLASSLOADER,
        PLUGINS
    }

    private static final Logger log = LoggerFactory.getLogger(Plugins.class);
    private final DelegatingClassLoader delegatingLoader;
    private final PluginScanResult scanResult;

    public Plugins(Map<String, String> props) {
        this(props, Plugins.class.getClassLoader(), new ClassLoaderFactory());
    }

    // VisibleForTesting
    @SuppressWarnings("this-escape")
    Plugins(Map<String, String> props, ClassLoader parent, ClassLoaderFactory factory) {
        String pluginPath = WorkerConfig.pluginPath(props);
        PluginDiscoveryMode discoveryMode = WorkerConfig.pluginDiscovery(props);
        Set<Path> pluginLocations = PluginUtils.pluginLocations(pluginPath, false);
        delegatingLoader = factory.newDelegatingClassLoader(parent);
        Set<PluginSource> pluginSources = PluginUtils.pluginSources(pluginLocations, delegatingLoader, factory);
        scanResult = initLoaders(pluginSources, discoveryMode);
    }

    public PluginScanResult initLoaders(Set<PluginSource> pluginSources, PluginDiscoveryMode discoveryMode) {
        PluginScanResult empty = new PluginScanResult(Collections.emptyList());
        PluginScanResult serviceLoadingScanResult;
        try {
            serviceLoadingScanResult = discoveryMode.serviceLoad() ?
                    new ServiceLoaderScanner().discoverPlugins(pluginSources) : empty;
        } catch (Throwable t) {
            throw new ConnectException(String.format(
                    "Unable to perform ServiceLoader scanning as requested by %s=%s. It may be possible to fix this issue by reconfiguring %s=%s",
                    WorkerConfig.PLUGIN_DISCOVERY_CONFIG, discoveryMode,
                    WorkerConfig.PLUGIN_DISCOVERY_CONFIG, PluginDiscoveryMode.ONLY_SCAN), t);
        }
        PluginScanResult reflectiveScanResult = discoveryMode.reflectivelyScan() ?
                new ReflectionScanner().discoverPlugins(pluginSources) : empty;
        PluginScanResult scanResult = new PluginScanResult(Arrays.asList(reflectiveScanResult, serviceLoadingScanResult));
        maybeReportHybridDiscoveryIssue(discoveryMode, serviceLoadingScanResult, scanResult);
        delegatingLoader.installDiscoveredPlugins(scanResult);
        return scanResult;
    }

    // visible for testing
    static void maybeReportHybridDiscoveryIssue(PluginDiscoveryMode discoveryMode, PluginScanResult serviceLoadingScanResult, PluginScanResult mergedResult) {
        SortedSet<PluginDesc<?>> missingPlugins = new TreeSet<>();
        mergedResult.forEach(missingPlugins::add);
        serviceLoadingScanResult.forEach(missingPlugins::remove);
        if (missingPlugins.isEmpty()) {
            if (discoveryMode == PluginDiscoveryMode.HYBRID_WARN || discoveryMode == PluginDiscoveryMode.HYBRID_FAIL) {
                log.warn("All plugins have ServiceLoader manifests, consider reconfiguring {}={}",
                        WorkerConfig.PLUGIN_DISCOVERY_CONFIG, PluginDiscoveryMode.SERVICE_LOAD);
            }
        } else {
            String message = String.format(
                "One or more plugins are missing ServiceLoader manifests may not be usable with %s=%s: %s%n" +
                        "Read the documentation at %s for instructions on migrating your plugins " +
                        "to take advantage of the performance improvements of %s mode.",
                            WorkerConfig.PLUGIN_DISCOVERY_CONFIG,
                    PluginDiscoveryMode.SERVICE_LOAD,
                    missingPlugins.stream()
                            .map(pluginDesc -> pluginDesc.location() + "\t" + pluginDesc.className() + "\t" + pluginDesc.type() + "\t" + pluginDesc.version())
                            .collect(Collectors.joining("\n", "[\n", "\n]")),
                    "https://kafka.apache.org/documentation.html#connect_plugindiscovery",
                    PluginDiscoveryMode.SERVICE_LOAD
            );
            if (discoveryMode == PluginDiscoveryMode.HYBRID_WARN) {
                log.warn("{} To silence this warning, set {}={} in the worker config.",
                        message, WorkerConfig.PLUGIN_DISCOVERY_CONFIG, PluginDiscoveryMode.ONLY_SCAN);
            } else if (discoveryMode == PluginDiscoveryMode.HYBRID_FAIL) {
                throw new ConnectException(String.format("%s To silence this error, set %s=%s in the worker config.",
                        message, WorkerConfig.PLUGIN_DISCOVERY_CONFIG, PluginDiscoveryMode.HYBRID_WARN));
            }
        }
    }

    private static <T> String pluginNames(Collection<PluginDesc<T>> plugins) {
        return plugins.stream().map(PluginDesc::toString).collect(Collectors.joining(", "));
    }

    private <T> T newPlugin(Class<T> klass) {
        // KAFKA-8340: The thread classloader is used during static initialization and must be
        // set to the plugin's classloader during instantiation
        try (LoaderSwap loaderSwap = withClassLoader(klass.getClassLoader())) {
            return Utils.newInstance(klass);
        } catch (Throwable t) {
            throw new ConnectException("Instantiation error", t);
        }
    }

    @SuppressWarnings("unchecked")
    protected <U> Class<? extends U> pluginClassFromConfig(
            AbstractConfig config,
            String propertyName,
            Class<U> pluginClass,
            Collection<PluginDesc<U>> plugins
    ) {
        Class<?> klass = config.getClass(propertyName);
        if (pluginClass.isAssignableFrom(klass)) {
            return (Class<? extends U>) klass;
        }
        throw new ConnectException(
            "Failed to find any class that implements " + pluginClass.getSimpleName()
                + " for the config "
                + propertyName + ", available classes are: "
                + pluginNames(plugins)
        );
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

    public Class<?> pluginClass(String classOrAlias) throws ClassNotFoundException {
        return pluginClass(delegatingLoader, classOrAlias, Object.class);
    }

    public static ClassLoader compareAndSwapLoaders(ClassLoader loader) {
        ClassLoader current = Thread.currentThread().getContextClassLoader();
        if (!current.equals(loader)) {
            Thread.currentThread().setContextClassLoader(loader);
        }
        return current;
    }

    public ClassLoader compareAndSwapWithDelegatingLoader() {
        ClassLoader current = Thread.currentThread().getContextClassLoader();
        if (!current.equals(delegatingLoader)) {
            Thread.currentThread().setContextClassLoader(delegatingLoader);
        }
        return current;
    }

    /**
     * Perform the following operations with a specified thread context classloader.
     * <p>
     * Intended for use in a try-with-resources block such as the following:
     * <pre>{@code
     * try (LoaderSwap loaderSwap = plugins.withClassLoader(loader)) {
     *     // operation(s) sensitive to the thread context classloader
     * }
     * }</pre>
     * After the completion of the try block, the previous context classloader will be restored.
     * @see Thread#getContextClassLoader()
     * @see LoaderSwap
     * @param loader ClassLoader to use as the thread context classloader
     * @return A {@link LoaderSwap} handle which restores the prior classloader on {@link LoaderSwap#close()}.
     */
    public LoaderSwap withClassLoader(ClassLoader loader) {
        ClassLoader savedLoader = compareAndSwapLoaders(loader);
        try {
            return new LoaderSwap(savedLoader);
        } catch (Throwable t) {
            compareAndSwapLoaders(savedLoader);
            throw t;
        }
    }

    /**
     * Wrap a {@link Runnable} such that it is performed with the specified thread context classloader
     * @see Thread#getContextClassLoader()
     * @param classLoader {@link ClassLoader} to use as the thread context classloader
     * @param operation {@link Runnable} which is sensitive to the thread context classloader
     * @return A wrapper {@link Runnable} which will execute the wrapped operation
     */
    public Runnable withClassLoader(ClassLoader classLoader, Runnable operation) {
        return () -> {
            try (LoaderSwap loaderSwap = withClassLoader(classLoader)) {
                operation.run();
            }
        };
    }

    public DelegatingClassLoader delegatingLoader() {
        return delegatingLoader;
    }

    public ClassLoader connectorLoader(String connectorClassOrAlias) {
        return delegatingLoader.connectorLoader(connectorClassOrAlias);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Set<PluginDesc<Connector>> connectors() {
        Set<PluginDesc<Connector>> connectors = new TreeSet<>((Set) sinkConnectors());
        connectors.addAll((Set) sourceConnectors());
        return connectors;
    }

    public Set<PluginDesc<SinkConnector>> sinkConnectors() {
        return scanResult.sinkConnectors();
    }

    public Set<PluginDesc<SourceConnector>> sourceConnectors() {
        return scanResult.sourceConnectors();
    }

    public Set<PluginDesc<Converter>> converters() {
        return scanResult.converters();
    }

    public Set<PluginDesc<HeaderConverter>> headerConverters() {
        return scanResult.headerConverters();
    }

    public Set<PluginDesc<ErrorRecordReporter<?>>> errorRecordReporters() {
        return scanResult.errorRecordReporters();
    }

    public Set<PluginDesc<Transformation<?>>> transformations() {
        return scanResult.transformations();
    }

    public Set<PluginDesc<Predicate<?>>> predicates() {
        return scanResult.predicates();
    }

    public Set<PluginDesc<ConnectorClientConfigOverridePolicy>> connectorClientConfigPolicies() {
        return scanResult.connectorClientConfigPolicies();
    }

    public Object newPlugin(String classOrAlias) throws ClassNotFoundException {
        Class<?> klass = pluginClass(delegatingLoader, classOrAlias, Object.class);
        return newPlugin(klass);
    }

    public Connector newConnector(String connectorClassOrAlias) {
        Class<? extends Connector> klass = connectorClass(connectorClassOrAlias);
        return newPlugin(klass);
    }

    public Class<? extends Connector> connectorClass(String connectorClassOrAlias) {
        Class<? extends Connector> klass;
        try {
            klass = pluginClass(
                    delegatingLoader,
                    connectorClassOrAlias,
                    Connector.class
            );
        } catch (ClassNotFoundException e) {
            List<PluginDesc<? extends Connector>> matches = new ArrayList<>();
            Set<PluginDesc<Connector>> connectors = connectors();
            for (PluginDesc<? extends Connector> plugin : connectors) {
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
                                + connectors.stream().map(PluginDesc::toString).collect(Collectors.joining(", "))
                );
            }
            if (matches.size() > 1) {
                throw new ConnectException(
                        "More than one connector matches alias "
                                + connectorClassOrAlias
                                + ". Please use full package and class name instead. Classes found: "
                                + connectors.stream().map(PluginDesc::toString).collect(Collectors.joining(", "))
                );
            }

            PluginDesc<? extends Connector> entry = matches.get(0);
            klass = entry.pluginClass();
        }
        return klass;
    }

    public Task newTask(Class<? extends Task> taskClass) {
        return newPlugin(taskClass);
    }

    /**
     * If the given configuration defines a {@link Converter} using the named configuration property, return a new configured instance.
     *
     * @param config             the configuration containing the {@link Converter}'s configuration; may not be null
     * @param classPropertyName  the name of the property that contains the name of the {@link Converter} class; may not be null
     * @param classLoaderUsage   which classloader should be used
     * @return the instantiated and configured {@link Converter}; null if the configuration did not define the specified property
     * @throws ConnectException if the {@link Converter} implementation class could not be found
     */
    public Converter newConverter(AbstractConfig config, String classPropertyName, ClassLoaderUsage classLoaderUsage) {
        if (!config.originals().containsKey(classPropertyName)) {
            // This configuration does not define the converter via the specified property name
            return null;
        }

        Class<? extends Converter> klass = null;
        switch (classLoaderUsage) {
            case CURRENT_CLASSLOADER:
                // Attempt to load first with the current classloader, and plugins as a fallback.
                // Note: we can't use config.getConfiguredInstance because Converter doesn't implement Configurable, and even if it did
                // we have to remove the property prefixes before calling config(...) and we still always want to call Converter.config.
                klass = pluginClassFromConfig(config, classPropertyName, Converter.class, scanResult.converters());
                break;
            case PLUGINS:
                // Attempt to load with the plugin class loader, which uses the current classloader as a fallback
                String converterClassOrAlias = config.getClass(classPropertyName).getName();
                try {
                    klass = pluginClass(delegatingLoader, converterClassOrAlias, Converter.class);
                } catch (ClassNotFoundException e) {
                    throw new ConnectException(
                            "Failed to find any class that implements Converter and which name matches "
                            + converterClassOrAlias + ", available converters are: "
                            + pluginNames(scanResult.converters())
                    );
                }
                break;
        }
        if (klass == null) {
            throw new ConnectException("Unable to initialize the Converter specified in '" + classPropertyName + "'");
        }

        // Determine whether this is a key or value converter based upon the supplied property name ...
        final boolean isKeyConverter = WorkerConfig.KEY_CONVERTER_CLASS_CONFIG.equals(classPropertyName);

        // Configure the Converter using only the old configuration mechanism ...
        String configPrefix = classPropertyName + ".";
        Map<String, Object> converterConfig = config.originalsWithPrefix(configPrefix);
        log.debug("Configuring the {} converter with configuration keys:{}{}",
                  isKeyConverter ? "key" : "value", System.lineSeparator(), converterConfig.keySet());

        Converter plugin;
        try (LoaderSwap loaderSwap = withClassLoader(klass.getClassLoader())) {
            plugin = newPlugin(klass);
            plugin.configure(converterConfig, isKeyConverter);
        }
        return plugin;
    }

    /**
     * Load an internal converter, used by the worker for (de)serializing data in internal topics.
     *
     * @param isKey           whether the converter is a key converter
     * @param className       the class name of the converter
     * @param converterConfig the properties to configure the converter with
     * @return the instantiated and configured {@link Converter}; never null
     * @throws ConnectException if the {@link Converter} implementation class could not be found
     */
    public Converter newInternalConverter(boolean isKey, String className, Map<String, String> converterConfig) {
        Class<? extends Converter> klass;
        try {
            klass = pluginClass(delegatingLoader, className, Converter.class);
        } catch (ClassNotFoundException e) {
            throw new ConnectException("Failed to load internal converter class " + className);
        }

        Converter plugin;
        try (LoaderSwap loaderSwap = withClassLoader(klass.getClassLoader())) {
            plugin = newPlugin(klass);
            plugin.configure(converterConfig, isKey);
        }
        return plugin;
    }

    /**
     * If the given configuration defines a {@link HeaderConverter} using the named configuration property, return a new configured
     * instance.
     *
     * @param config             the configuration containing the {@link Converter}'s configuration; may not be null
     * @param classPropertyName  the name of the property that contains the name of the {@link Converter} class; may not be null
     * @param classLoaderUsage   which classloader should be used
     * @return the instantiated and configured {@link HeaderConverter}; null if the configuration did not define the specified property
     * @throws ConnectException if the {@link HeaderConverter} implementation class could not be found
     */
    public HeaderConverter newHeaderConverter(AbstractConfig config, String classPropertyName, ClassLoaderUsage classLoaderUsage) {
        Class<? extends HeaderConverter> klass = null;
        switch (classLoaderUsage) {
            case CURRENT_CLASSLOADER:
                if (!config.originals().containsKey(classPropertyName)) {
                    // This connector configuration does not define the header converter via the specified property name
                    return null;
                }
                // Attempt to load first with the current classloader, and plugins as a fallback.
                // Note: we can't use config.getConfiguredInstance because we have to remove the property prefixes
                // before calling config(...)
                klass = pluginClassFromConfig(config, classPropertyName, HeaderConverter.class, scanResult.headerConverters());
                break;
            case PLUGINS:
                // Attempt to load with the plugin class loader, which uses the current classloader as a fallback.
                // Note that there will always be at least a default header converter for the worker
                String converterClassOrAlias = config.getClass(classPropertyName).getName();
                try {
                    klass = pluginClass(
                            delegatingLoader,
                            converterClassOrAlias,
                            HeaderConverter.class
                    );
                } catch (ClassNotFoundException e) {
                    throw new ConnectException(
                            "Failed to find any class that implements HeaderConverter and which name matches "
                                    + converterClassOrAlias
                                    + ", available header converters are: "
                                    + pluginNames(scanResult.headerConverters())
                    );
                }
        }
        if (klass == null) {
            throw new ConnectException("Unable to initialize the HeaderConverter specified in '" + classPropertyName + "'");
        }

        String configPrefix = classPropertyName + ".";
        Map<String, Object> converterConfig = config.originalsWithPrefix(configPrefix);
        converterConfig.put(ConverterConfig.TYPE_CONFIG, ConverterType.HEADER.getName());
        log.debug("Configuring the header converter with configuration keys:{}{}", System.lineSeparator(), converterConfig.keySet());

        HeaderConverter plugin;
        try (LoaderSwap loaderSwap = withClassLoader(klass.getClassLoader())) {
            plugin = newPlugin(klass);
            plugin.configure(converterConfig);
        }
        return plugin;
    }

    public ConfigProvider newConfigProvider(AbstractConfig config, String providerPrefix, ClassLoaderUsage classLoaderUsage) {
        String classPropertyName = providerPrefix + ".class";
        Map<String, String> originalConfig = config.originalsStrings();
        if (!originalConfig.containsKey(classPropertyName)) {
            // This configuration does not define the config provider via the specified property name
            return null;
        }
        Class<? extends ConfigProvider> klass = null;
        switch (classLoaderUsage) {
            case CURRENT_CLASSLOADER:
                // Attempt to load first with the current classloader, and plugins as a fallback.
                klass = pluginClassFromConfig(config, classPropertyName, ConfigProvider.class, scanResult.configProviders());
                break;
            case PLUGINS:
                // Attempt to load with the plugin class loader, which uses the current classloader as a fallback
                String configProviderClassOrAlias = originalConfig.get(classPropertyName);
                try {
                    klass = pluginClass(delegatingLoader, configProviderClassOrAlias, ConfigProvider.class);
                } catch (ClassNotFoundException e) {
                    throw new ConnectException(
                            "Failed to find any class that implements ConfigProvider and which name matches "
                                    + configProviderClassOrAlias + ", available ConfigProviders are: "
                                    + pluginNames(scanResult.configProviders())
                    );
                }
                break;
        }
        if (klass == null) {
            throw new ConnectException("Unable to initialize the ConfigProvider specified in '" + classPropertyName + "'");
        }

        // Configure the ConfigProvider
        String configPrefix = providerPrefix + ".param.";
        Map<String, Object> configProviderConfig = config.originalsWithPrefix(configPrefix);

        ConfigProvider plugin;
        try (LoaderSwap loaderSwap = withClassLoader(klass.getClassLoader())) {
            plugin = newPlugin(klass);
            plugin.configure(configProviderConfig);
        }
        return plugin;
    }

    /**
     * If the given class names are available in the classloader, return a list of new configured
     * instances. If the instances implement {@link Configurable}, they are configured with provided {@param config}
     *
     * @param klassNames         the list of class names of plugins that needs to instantiated and configured
     * @param config             the configuration containing the {@link org.apache.kafka.connect.runtime.Worker}'s configuration; may not be {@code null}
     * @param pluginKlass        the type of the plugin class that is being instantiated
     * @return the instantiated and configured list of plugins of type <T>; empty list if the {@param klassNames} is {@code null} or empty
     * @throws ConnectException if the implementation class could not be found
     */
    public <T> List<T> newPlugins(List<String> klassNames, AbstractConfig config, Class<T> pluginKlass) {
        List<T> plugins = new ArrayList<>();
        if (klassNames != null) {
            for (String klassName : klassNames) {
                plugins.add(newPlugin(klassName, config, pluginKlass));
            }
        }
        return plugins;
    }

    public <T> T newPlugin(String klassName, AbstractConfig config, Class<T> pluginKlass) {
        T plugin;
        Class<? extends T> klass;
        try {
            klass = pluginClass(delegatingLoader, klassName, pluginKlass);
        } catch (ClassNotFoundException e) {
            String msg = String.format("Failed to find any class that implements %s and which "
                                       + "name matches %s", pluginKlass, klassName);
            throw new ConnectException(msg);
        }
        try (LoaderSwap loaderSwap = withClassLoader(klass.getClassLoader())) {
            plugin = newPlugin(klass);
            if (plugin instanceof Versioned) {
                Versioned versionedPlugin = (Versioned) plugin;
                if (Utils.isBlank(versionedPlugin.version())) {
                    throw new ConnectException("Version not defined for '" + klassName + "'");
                }
            }
            if (plugin instanceof Configurable) {
                ((Configurable) plugin).configure(config.originals());
            }
        }
        return plugin;
    }

}
