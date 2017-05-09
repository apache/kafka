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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class DelegatingClassLoader extends URLClassLoader {
    private static final Logger log = LoggerFactory.getLogger(DelegatingClassLoader.class);
    private static final String[] EXCLUSIONS = {
            "java.",
            "javax.",
            "org.apache.kafka.",
            "org.apache.log4j."
    };

    private final Map<String, SortedMap<ModuleDesc, ModuleClassLoader>> moduleLoaders;
    private final List<String> modulePaths;
    private final Map<Path, ModuleClassLoader> activePaths;
    private final Map<Path, Void> inactivePaths;

    public DelegatingClassLoader(List<String> modulePaths, ClassLoader parent) {
        super(new URL[0], parent);
        this.modulePaths = modulePaths;
        this.moduleLoaders = new HashMap<>();
        this.activePaths = new HashMap<>();
        this.inactivePaths = new HashMap<>();
        initLoaders();
    }

    public DelegatingClassLoader(List<String> modulePaths) {
        this(modulePaths, ClassLoader.getSystemClassLoader());
    }

    public void initLoaders() {
        for (String path : modulePaths) {
            try {
                Path modulePath = Paths.get(path).toAbsolutePath();
                if (Files.isDirectory(modulePath)) {
                    URL[] jars = getJarPaths(modulePath).toArray(new URL[0]);
                    ModuleClassLoader loader = new ModuleClassLoader(jars);
                    List<ModuleDesc> modules = scanModulePath(loader, jars);

                    if (modules.isEmpty()) {
                        inactivePaths.put(modulePath, null);
                    } else {
                        activePaths.put(modulePath, loader);
                    }

                    for (ModuleDesc module : modules) {
                        String moduleClassName = module.className();
                        SortedMap<ModuleDesc, ModuleClassLoader> inner =
                                moduleLoaders.get(moduleClassName);
                        if (inner == null) {
                            inner = new TreeMap<>();
                            moduleLoaders.put(moduleClassName, inner);
                        }
                        inner.put(module, loader);
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

    public List<ModuleDesc> scanModulePath(
            ModuleClassLoader loader,
            URL[] urls
    ) throws InstantiationException, IllegalAccessException {
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.setClassLoaders(new ModuleClassLoader[]{loader});
        builder.addUrls(urls);
        Reflections reflections = new Reflections(builder);

        List<ModuleDesc> modules = new ArrayList<>();
        Class<?>[] moduleClasses = {Connector.class, Converter.class, Transformation.class};
        for (Class<?> moduleClass : moduleClasses) {
            modules.addAll(getModuleDesc(reflections, moduleClass));
        }
        return modules;
    }

    public <T> List<ModuleDesc> getModuleDesc(
            Reflections reflections,
            Class<T> klass
    ) throws InstantiationException, IllegalAccessException {
        Set<Class<? extends T>> modules = reflections.getSubTypesOf(klass);

        List<ModuleDesc> result = new ArrayList<>();
        for (Class<? extends T> module : modules) {
            if (isConcrete(module)) {
                if (Connector.class.isAssignableFrom(module)) {
                    result.add(
                            new ModuleDesc(module, ((Connector) module.newInstance()).version())
                    );
                } else {
                    result.add(new ModuleDesc(module, "undefined"));
                }
            }
        }
        return result;
    }


    private static boolean isConcrete(Class<?> cls) {
        final int mod = cls.getModifiers();
        return !Modifier.isAbstract(mod) && !Modifier.isInterface(mod);
    }

    private static List<URL> getJarPaths(Path topDir) throws IOException {
        List<URL> jars = new ArrayList<>();
        DirectoryStream.Filter<Path> jarFilter = new DirectoryStream.Filter<Path>() {
            public boolean accept(Path file) throws IOException {
                return file.toString().toLowerCase().endsWith(".jar");
            }
        };

        // Non-recursive for now
        try (DirectoryStream<Path> listing = Files.newDirectoryStream(topDir, jarFilter)) {
            for (Path jar : listing) {
                jars.add(jar.toUri().toURL());
            }
        }
        return jars;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (!validate(name)) {
            // There are no paths in this classloader, will attempt to load with the parent.
            return super.loadClass(name, resolve);
        }

        SortedMap<ModuleDesc, ModuleClassLoader> inner = moduleLoaders.get(name);
        if (inner != null) {
            return inner.get(inner.lastKey()).loadClass(name, resolve);
        }

        Class<?> klass = null;
        for (ModuleClassLoader loader : activePaths.values()) {
            try {
                klass = loader.loadClass(name, resolve);
            } catch (ClassNotFoundException e) {
                // Not found in this loader.
            }
        }
        if (klass == null) {
            return super.loadClass(name, resolve);
        }
        return klass;
    }

    protected boolean validate(String name) {
        boolean result = false;
        for (String exclusion : EXCLUSIONS) {
            result |= name.startsWith(exclusion);
        }
        return !result;
    }
}
