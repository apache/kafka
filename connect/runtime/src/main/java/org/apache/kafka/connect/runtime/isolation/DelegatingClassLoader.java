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

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.reflections.Configuration;
import org.reflections.Reflections;
import org.reflections.ReflectionsException;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Driver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class DelegatingClassLoader extends URLClassLoader {
    private static final Logger log = LoggerFactory.getLogger(DelegatingClassLoader.class);
    private static final String CLASSPATH_NAME = "classpath";

    private final ConcurrentMap<String, ConcurrentNavigableMap<PluginDesc<?>, ClassLoader>> pluginLoaders;
    private final Map<String, String> aliases;
    private final SortedSet<PluginDesc<Connector>> connectors;
    private final SortedSet<PluginDesc<Converter>> converters;
    private final SortedSet<PluginDesc<HeaderConverter>> headerConverters;
    private final SortedSet<PluginDesc<Transformation>> transformations;
    private final List<String> pluginPaths;
    private final ConcurrentMap<Path, PluginClassLoader> activePaths;

    public DelegatingClassLoader(List<String> pluginPaths, ClassLoader parent) {
        super(new URL[0], parent);
        this.pluginPaths = pluginPaths;
        this.pluginLoaders = new ConcurrentHashMap<>();
        this.aliases = new HashMap<>();
        this.activePaths = new ConcurrentHashMap<>();
        this.connectors = new ConcurrentSkipListSet<>();
        this.converters = new ConcurrentSkipListSet<>();
        this.headerConverters = new ConcurrentSkipListSet<>();
        this.transformations = new ConcurrentSkipListSet<>();
    }

    public DelegatingClassLoader(List<String> pluginPaths) {
        this(pluginPaths, ClassLoader.getSystemClassLoader());
    }

    public Set<PluginDesc<Connector>> connectors() {
        return connectors;
    }

    public Set<PluginDesc<Converter>> converters() {
        return converters;
    }

    public Set<PluginDesc<HeaderConverter>> headerConverters() {
        return headerConverters;
    }

    public Set<PluginDesc<Transformation>> transformations() {
        return transformations;
    }

    public ClassLoader connectorLoader(Connector connector) {
        return connectorLoader(connector.getClass().getName());
    }

    public ClassLoader connectorLoader(String connectorClassOrAlias) {
        log.debug("Getting plugin class loader for connector: '{}'", connectorClassOrAlias);
        String fullName = getFullName(connectorClassOrAlias);
        ConcurrentNavigableMap<PluginDesc<?>, ClassLoader> inner = pluginLoaders.get(fullName);
        if (inner == null) {
            log.error(
                    "Plugin class loader for connector: '{}' was not found. Returning: {}",
                    connectorClassOrAlias,
                    this
            );
            return this;
        }
        return inner.lastEntry().getValue();
    }

    private static PluginClassLoader newPluginClassLoader(
            final URL pluginLocation,
            final URL[] urls,
            final ClassLoader parent
    ) {
        return (PluginClassLoader) AccessController.doPrivileged(
                new PrivilegedAction() {
                    @Override
                    public Object run() {
                        return new PluginClassLoader(pluginLocation, urls, parent);
                    }
                }
        );
    }

    private <T> void addPlugins(Collection<PluginDesc<T>> plugins, ClassLoader loader) {
        for (PluginDesc<T> plugin : plugins) {
            String pluginClassName = plugin.className();
            // TODO: Once migration to Java 8 is complete, use a Supplier<TreeMap> to avoid
            // unnecessarily creating a new map here each time this method is called; right now we
            // need to create one in order to use the atomic ConcurrentMap.putIfAbsent(...) method
            ConcurrentNavigableMap<PluginDesc<?>, ClassLoader> inner = new ConcurrentSkipListMap<>();
            ConcurrentNavigableMap<PluginDesc<?>, ClassLoader> priorInner =
                pluginLoaders.putIfAbsent(pluginClassName, inner);
            if (priorInner == null) {
                // TODO: once versioning is enabled this line should be moved outside this if branch
                log.info("Added plugin '{}'", pluginClassName);
            } else {
                inner = priorInner;
            }
            inner.put(plugin, loader);
        }
    }

    protected void initLoaders() {
        List<Path> pluginPathDirectories = new ArrayList<>();
        for (String configPath : pluginPaths) {
            initPluginLoader(configPath);
            Path pluginPath = Paths.get(configPath);
            if (Files.isDirectory(pluginPath)) {
                pluginPathDirectories.add(pluginPath);
            }
        }
        // Finally add parent/system loader.
        initPluginLoader(CLASSPATH_NAME);
        updateAliases();
        if (!pluginPathDirectories.isEmpty()) {
            registerFileSystemWatcher(pluginPathDirectories);
        }
    }

    private void registerFileSystemWatcher(Iterable<Path> directories) {
        PluginPathDirectoryListener pluginPathDirectoryListener;
        try {
            pluginPathDirectoryListener = new PluginPathDirectoryListener(new PluginReceiver(), directories);
        } catch (IOException e) {
            log.warn(
                "Unable to initialize file system watch service; plugins will not be loaded dynamically",
                e
            );
            return;
        }
        Thread pluginDirectoryListenerThread = new Thread(pluginPathDirectoryListener);
        pluginDirectoryListenerThread.setDaemon(true);
        pluginDirectoryListenerThread.start();
    }

    private void initPluginLoader(String path) {
        try {
            if (CLASSPATH_NAME.equals(path)) {
                scanUrlsAndAddPlugins(
                        getParent(),
                        ClasspathHelper.forJavaClassPath().toArray(new URL[0]),
                        null
                );
            } else {
                Path pluginPath = Paths.get(path).toAbsolutePath();
                // Update for exception handling
                path = pluginPath.toString();
                // Currently 'plugin.paths' property is a list of top-level directories
                // containing plugins
                if (Files.isDirectory(pluginPath)) {
                    for (Path pluginLocation : PluginUtils.pluginLocations(pluginPath)) {
                        registerPlugin(pluginLocation);
                    }
                } else if (PluginUtils.isArchive(pluginPath)) {
                    registerPlugin(pluginPath);
                }
            }
        } catch (InvalidPathException | MalformedURLException e) {
            log.error("Invalid path in plugin path: {}. Ignoring.", path, e);
        } catch (IOException e) {
            log.error("Could not get listing for plugin path: {}. Ignoring.", path, e);
        } catch (InstantiationException | IllegalAccessException e) {
            log.error("Could not instantiate plugins in: {}. Ignoring: {}", path, e);
        }
    }

    private void registerPlugin(Path pluginLocation)
            throws InstantiationException, IllegalAccessException, IOException {
        log.info("Loading plugin from: {}", pluginLocation);
        List<URL> pluginUrls = new ArrayList<>();
        for (Path path : PluginUtils.pluginUrls(pluginLocation)) {
            pluginUrls.add(path.toUri().toURL());
        }
        URL[] urls = pluginUrls.toArray(new URL[0]);
        if (log.isDebugEnabled()) {
            log.debug("Loading plugin urls: {}", Arrays.toString(urls));
        }
        PluginClassLoader loader = newPluginClassLoader(
                pluginLocation.toUri().toURL(),
                urls,
                this
        );
        scanUrlsAndAddPlugins(loader, urls, pluginLocation);
    }

    private void scanUrlsAndAddPlugins(
            ClassLoader loader,
            URL[] urls,
            Path pluginLocation
    ) throws InstantiationException, IllegalAccessException {
        PluginScanResult plugins = scanPluginPath(loader, urls);
        log.info("Registered loader: {}", loader);
        if (!plugins.isEmpty()) {
            if (loader instanceof PluginClassLoader) {
                activePaths.put(pluginLocation, (PluginClassLoader) loader);
            }

            addPlugins(plugins.connectors(), loader);
            connectors.addAll(plugins.connectors());
            addPlugins(plugins.converters(), loader);
            converters.addAll(plugins.converters());
            addPlugins(plugins.headerConverters(), loader);
            headerConverters.addAll(plugins.headerConverters());
            addPlugins(plugins.transformations(), loader);
            transformations.addAll(plugins.transformations());
        }
        loadJdbcDrivers(loader);
    }

    private void loadJdbcDrivers(final ClassLoader loader) {
        // Apply here what java.sql.DriverManager does to discover and register classes
        // implementing the java.sql.Driver interface.
        AccessController.doPrivileged(
                new PrivilegedAction<Void>() {
                    public Void run() {
                        ServiceLoader<Driver> loadedDrivers = ServiceLoader.load(
                                Driver.class,
                                loader
                        );
                        Iterator<Driver> driversIterator = loadedDrivers.iterator();
                        try {
                            while (driversIterator.hasNext()) {
                                Driver driver = driversIterator.next();
                                log.debug(
                                        "Registered java.sql.Driver: {} to java.sql.DriverManager",
                                        driver
                                );
                            }
                        } catch (Throwable t) {
                            log.debug(
                                    "Ignoring java.sql.Driver classes listed in resources but not"
                                            + " present in class loader's classpath: ",
                                    t
                            );
                        }
                        return null;
                    }
                }
        );
    }

    private PluginScanResult scanPluginPath(
            ClassLoader loader,
            URL[] urls
    ) throws InstantiationException, IllegalAccessException {
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.setClassLoaders(new ClassLoader[]{loader});
        builder.addUrls(urls);
        builder.setScanners(new SubTypesScanner());
        builder.useParallelExecutor();
        Reflections reflections = new InternalReflections(builder);

        return new PluginScanResult(
                getPluginDesc(reflections, Connector.class, loader),
                getPluginDesc(reflections, Converter.class, loader),
                getPluginDesc(reflections, HeaderConverter.class, loader),
                getPluginDesc(reflections, Transformation.class, loader)
        );
    }

    private <T> Collection<PluginDesc<T>> getPluginDesc(
            Reflections reflections,
            Class<T> klass,
            ClassLoader loader
    ) throws InstantiationException, IllegalAccessException {
        Set<Class<? extends T>> plugins = reflections.getSubTypesOf(klass);

        Collection<PluginDesc<T>> result = new ArrayList<>();
        for (Class<? extends T> plugin : plugins) {
            if (PluginUtils.isConcrete(plugin)) {
                // Temporary workaround until all the plugins are versioned.
                if (Connector.class.isAssignableFrom(plugin)) {
                    result.add(
                            new PluginDesc<>(
                                    plugin,
                                    ((Connector) plugin.newInstance()).version(),
                                    loader
                            )
                    );
                } else {
                    result.add(new PluginDesc<>(plugin, "undefined", loader));
                }
            }
        }
        return result;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (!PluginUtils.shouldLoadInIsolation(name)) {
            // There are no paths in this classloader, will attempt to load with the parent.
            return super.loadClass(name, resolve);
        }

        String fullName = getFullName(name);
        ConcurrentNavigableMap<PluginDesc<?>, ClassLoader> inner = pluginLoaders.get(fullName);
        if (inner != null) {
            ClassLoader pluginLoader = inner.lastEntry().getValue();
            log.trace("Retrieving loaded class '{}' from '{}'", fullName, pluginLoader);
            return pluginLoader instanceof PluginClassLoader
                   ? ((PluginClassLoader) pluginLoader).loadClass(fullName, resolve)
                   : super.loadClass(fullName, resolve);
        }

        return super.loadClass(fullName, resolve);
    }

    private String getFullName(String name) {
        String aliasLookup;
        synchronized (aliases) {
            aliasLookup = aliases.get(name);
        }
        return aliasLookup != null ? aliasLookup : name;
    }

    // Could iterate over existing aliases and detect conflict with newly-added aliases, but simply
    // recalculating new aliases from scratch is easier to implement and probably won't create a
    // performance bottleneck
    private void updateAliases() {
        Map<String, String> newAliases = getAllAliases();
        synchronized (aliases) {
            aliases.clear();
            aliases.putAll(newAliases);
        }
    }

    private Map<String, String> getAllAliases() {
        Map<String, String> result = new HashMap<>();
        result.putAll(getAliases(connectors));
        result.putAll(getAliases(converters));
        result.putAll(getAliases(headerConverters));
        result.putAll(getAliases(transformations));
        return result;
    }

    private <S> Map<String, String> getAliases(Collection<PluginDesc<S>> plugins) {
        Map<String, String> result = new HashMap<>();
        for (PluginDesc<S> plugin : plugins) {
            if (PluginUtils.isAliasUnique(plugin, plugins)) {
                String simple = PluginUtils.simpleName(plugin);
                String pruned = PluginUtils.prunedName(plugin);
                result.put(simple, plugin.className());
                if (simple.equals(pruned)) {
                    log.info("Adding alias '{}' to plugin '{}'", simple, plugin.className());
                } else {
                    result.put(pruned, plugin.className());
                    log.info(
                            "Adding aliases '{}' and '{}' to plugin '{}'",
                            simple,
                            pruned,
                            plugin.className()
                    );
                }
            }
        }
        return result;
    }

    private static class InternalReflections extends Reflections {

        public InternalReflections(Configuration configuration) {
            super(configuration);
        }

        // When Reflections is used for parallel scans, it has a bug where it propagates ReflectionsException
        // as RuntimeException.  Override the scan behavior to emulate the singled-threaded logic.
        @Override
        protected void scan(URL url) {
            try {
                super.scan(url);
            } catch (ReflectionsException e) {
                Logger log = Reflections.log;
                if (log != null && log.isWarnEnabled()) {
                    log.warn("could not create Vfs.Dir from url. ignoring the exception and continuing", e);
                }
            }
        }
    }

    private class PluginReceiver implements PluginPathDirectoryListener.PluginReceiver {
        @Override
        public void onCreate(Path pluginLocation) {
            try {
                log.info("Entry {} on plugin path created; adding its plugins", pluginLocation);
                registerPlugin(pluginLocation);
                updateAliases();
            } catch (IOException e) {
                log.error(
                    "Could not get listing for newly-discovered plugin path: {}. Ignoring.",
                    pluginLocation,
                    e
                );
            } catch (InstantiationException | IllegalAccessException e) {
                log.error(
                    "Could not instantiate newly-discovered plugins in: {}. Ignoring: {}",
                    pluginLocation,
                    e
                );
            }
        }

        @Override
        public void onDelete(Path pluginLocation) {
            // On some operating systems, plugins will actually be available even after the
            // directory containing their class files has been deleted.
            log.warn(
                "Entry {} on plugin path removed; plugins contained contained in it may no longer be accessible",
                pluginLocation
            );
        }
    }
}
