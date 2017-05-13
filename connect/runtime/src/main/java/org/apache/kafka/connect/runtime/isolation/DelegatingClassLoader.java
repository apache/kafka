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
import org.apache.kafka.connect.transforms.Transformation;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DelegatingClassLoader extends URLClassLoader {
    private static final Logger log = LoggerFactory.getLogger(DelegatingClassLoader.class);

    private final ConcurrentMap<String, SortedMap<ModuleDesc<?>, ModuleClassLoader>> moduleLoaders;
    private final SortedSet<ModuleDesc<Connector>> connectors;
    private final SortedSet<ModuleDesc<Converter>> converters;
    private final SortedSet<ModuleDesc<Transformation>> transformations;
    private final List<String> modulePaths;
    private final Map<Path, ModuleClassLoader> activePaths;
    private final Map<Path, Void> inactivePaths;

    public DelegatingClassLoader(List<String> modulePaths, ClassLoader parent) {
        super(new URL[0], parent);
        this.modulePaths = modulePaths;
        this.moduleLoaders = new ConcurrentHashMap<>();
        this.activePaths = new HashMap<>();
        this.inactivePaths = new HashMap<>();
        this.connectors = new TreeSet<>();
        this.converters = new TreeSet<>();
        this.transformations = new TreeSet<>();
    }

    public DelegatingClassLoader(List<String> modulePaths) {
        this(modulePaths, ClassLoader.getSystemClassLoader());
    }

    private static boolean isConcrete(Class<?> cls) {
        final int mod = cls.getModifiers();
        return !Modifier.isAbstract(mod) && !Modifier.isInterface(mod);
    }

    private static List<Path> moduleDirs(Path topDir) throws IOException {
        DirectoryStream.Filter<Path> dirFilter = new DirectoryStream.Filter<Path>() {
            public boolean accept(Path path) throws IOException {
                return Files.isDirectory(path);
            }
        };

        List<Path> dirs = new ArrayList<>();
        // Non-recursive for now
        try (DirectoryStream<Path> listing = Files.newDirectoryStream(topDir, dirFilter)) {
            for (Path dir : listing) {
                dirs.add(dir);
            }
        }
        return dirs;
    }

    private static List<URL> jarPaths(Path moduleDir) throws IOException {
        DirectoryStream.Filter<Path> jarFilter = new DirectoryStream.Filter<Path>() {
            public boolean accept(Path file) throws IOException {
                return file.toString().toLowerCase(Locale.ROOT).endsWith(".jar");
            }
        };

        List<URL> jars = new ArrayList<>();
        try (DirectoryStream<Path> listing = Files.newDirectoryStream(moduleDir, jarFilter)) {
            for (Path jar : listing) {
                jars.add(jar.toUri().toURL());
            }
        }
        return jars;
    }

    public void addAlias(ModuleDesc<?> module, String alias) {
        SortedMap<ModuleDesc<?>, ModuleClassLoader> inner = moduleLoaders.get(module.className());
        if (inner != null) {
            moduleLoaders.putIfAbsent(alias, inner);
        }
    }

    private <T> void addModules(Collection<ModuleDesc<T>> modules, ModuleClassLoader loader) {
        for (ModuleDesc<T> module : modules) {
            String moduleClassName = module.className();
            SortedMap<ModuleDesc<?>, ModuleClassLoader> inner = moduleLoaders.get(moduleClassName);
            if (inner == null) {
                inner = new TreeMap<>();
                moduleLoaders.put(moduleClassName, inner);
            }
            inner.put(module, loader);
        }
    }

    public void initLoaders() {
        for (String path : modulePaths) {
            try {
                Path modulePath = Paths.get(path).toAbsolutePath();
                if (Files.isDirectory(modulePath)) {
                    for (Path dir : moduleDirs(modulePath)) {
                        log.info("Loading dir: {}", dir);
                        URL[] jars = jarPaths(dir).toArray(new URL[0]);
                        // log.info("Loading jars: " + Arrays.toString(jars));
                        ModuleClassLoader loader = newModuleClassLoader(jars);
                        log.info("Using loader: {}", loader);
                        ModuleScanResult modules = scanModulePath(loader, jars);

                        if (modules.isEmpty()) {
                            inactivePaths.put(dir, null);
                        } else {
                            activePaths.put(dir, loader);
                        }

                        addModules(modules.connectors(), loader);
                        connectors.addAll(modules.connectors());
                        addModules(modules.converters(), loader);
                        converters.addAll(modules.converters());
                        addModules(modules.transformations(), loader);
                        transformations.addAll(modules.transformations());
                    }
                }
            } catch (InvalidPathException | MalformedURLException e) {
                log.warn("Invalid path in module path: {}. Ignoring.", path);
            } catch (IOException e) {
                log.warn("Could not get listing for module path: {}. Ignoring.", path);
            } catch (InstantiationException | IllegalAccessException e) {
                log.warn("Could not instantiate modules in: {}. Ignoring: {}", path, e);
            }
        }
    }

    private static ModuleClassLoader newModuleClassLoader(final URL[] jars) {
        return (ModuleClassLoader) AccessController.doPrivileged(
                new PrivilegedAction() {
                    @Override
                    public Object run() {
                        return new ModuleClassLoader(jars);
                    }
                }
        );
    }

    public Set<ModuleDesc<Connector>> connectors() {
        return connectors;
    }

    public Set<ModuleDesc<Converter>> converters() {
        return converters;
    }

    public Set<ModuleDesc<Transformation>> transformations() {
        return transformations;
    }

    private ModuleScanResult scanModulePath(
            ModuleClassLoader loader,
            URL[] urls
    ) throws InstantiationException, IllegalAccessException {
        //log.info("Scanning module path urls: " + Arrays.toString(urls));
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.setClassLoaders(new ModuleClassLoader[]{loader});
        builder.addUrls(urls);
        Reflections reflections = new Reflections(builder);

        return new ModuleScanResult(
                getModuleDesc(reflections, Connector.class, loader),
                getModuleDesc(reflections, Converter.class, loader),
                getModuleDesc(reflections, Transformation.class, loader)
        );
    }

    private <T> Collection<ModuleDesc<T>> getModuleDesc(
            Reflections reflections,
            Class<T> klass,
            ModuleClassLoader loader
    ) throws InstantiationException, IllegalAccessException {
        Set<Class<? extends T>> modules = reflections.getSubTypesOf(klass);

        Collection<ModuleDesc<T>> result = new ArrayList<>();
        for (Class<? extends T> module : modules) {
            if (isConcrete(module)) {
                // Temporary workaround until all the modules are versioned.
                if (Connector.class.isAssignableFrom(module)) {
                    result.add(
                            new ModuleDesc<>(
                                    module,
                                    ((Connector) module.newInstance()).version(),
                                    loader
                            )
                    );
                } else {
                    result.add(new ModuleDesc<>(module, "undefined", loader));
                }
            }
        }
        return result;
    }

    public ModuleClassLoader connectorLoader(Connector connector) {
        return connectorLoader(connector.getClass().getCanonicalName());
    }

    public ModuleClassLoader connectorLoader(String connectorClassOrAlias) {
        SortedMap<ModuleDesc<?>, ModuleClassLoader> inner =
                moduleLoaders.get(connectorClassOrAlias);
        return inner.get(inner.lastKey());
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (name.startsWith("io.confluent.common")) {
            log.debug("Loading class: " + name);
        }

        if (!ModuleUtils.validate(name)) {
            // There are no paths in this classloader, will attempt to load with the parent.
            return super.loadClass(name, resolve);
        }

        SortedMap<ModuleDesc<?>, ModuleClassLoader> inner = moduleLoaders.get(name);
        if (inner != null) {
            log.warn("Class has been found before: {} by {}", name, inner.get(inner.lastKey()));
            return inner.get(inner.lastKey()).loadClass(name, resolve);
        }

        Class<?> klass = null;
        for (ModuleClassLoader loader : activePaths.values()) {
            try {
                klass = loader.loadClass(name, resolve);
                break;
            } catch (ClassNotFoundException e) {
                // Not found in this loader.
            }
        }
        if (klass == null) {
            return super.loadClass(name, resolve);
        }
        return klass;
    }
}
