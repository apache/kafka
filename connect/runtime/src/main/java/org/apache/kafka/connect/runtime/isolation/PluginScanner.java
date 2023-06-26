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

import org.apache.kafka.connect.components.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Driver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public abstract class PluginScanner {

    private static final Logger log = LoggerFactory.getLogger(PluginScanner.class);

    public PluginScanResult discoverPlugins(Set<PluginSource> sources) {
        long startMs = System.currentTimeMillis();
        List<PluginScanResult> results = new ArrayList<>();
        for (PluginSource source : sources) {
            results.add(scanUrlsAndAddPlugins(source));
        }
        long endMs = System.currentTimeMillis();
        log.info("Scanning plugins with {} took {} ms", getClass().getSimpleName(), endMs - startMs);
        return new PluginScanResult(results);
    }

    private PluginScanResult scanUrlsAndAddPlugins(PluginSource source) {
        log.info("Loading plugin from: {}", source.location());
        if (log.isDebugEnabled()) {
            log.debug("Loading plugin urls: {}", Arrays.toString(source.urls()));
        }
        PluginScanResult plugins = scanPlugins(source);
        log.info("Registered loader: {}", source.loader());
        loadJdbcDrivers(source.loader());
        return plugins;
    }

    protected abstract PluginScanResult scanPlugins(PluginSource source);

    private void loadJdbcDrivers(final ClassLoader loader) {
        // Apply here what java.sql.DriverManager does to discover and register classes
        // implementing the java.sql.Driver interface.
        AccessController.doPrivileged(
            (PrivilegedAction<Void>) () -> {
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
        );
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected <T> PluginDesc<T> pluginDesc(Class<? extends T> plugin, String version, ClassLoader loader) {
        return new PluginDesc(plugin, version, loader);
    }

    @SuppressWarnings("unchecked")
    protected <T> SortedSet<PluginDesc<T>> getServiceLoaderPluginDesc(Class<T> klass, ClassLoader loader) {
        SortedSet<PluginDesc<T>> result = new TreeSet<>();
        ServiceLoader<T> serviceLoader = ServiceLoader.load(klass, loader);
        for (Iterator<T> iterator = serviceLoader.iterator(); iterator.hasNext(); ) {
            try (LoaderSwap loaderSwap = withClassLoader(loader)) {
                T pluginImpl;
                try {
                    pluginImpl = iterator.next();
                } catch (ServiceConfigurationError t) {
                    log.error("Failed to discover {}{}", klass.getSimpleName(), reflectiveErrorDescription(t.getCause()), t);
                    continue;
                }
                Class<? extends T> pluginKlass = (Class<? extends T>) pluginImpl.getClass();
                if (pluginKlass.getClassLoader() != loader) {
                    log.debug("{} from other classloader {} is visible from {}, excluding to prevent isolated loading",
                            pluginKlass.getSimpleName(), pluginKlass.getClassLoader(), loader);
                    continue;
                }
                result.add(pluginDesc(pluginKlass, versionFor(pluginImpl), loader));
            }
        }
        return result;
    }

    protected static <T> String versionFor(T pluginImpl) {
        try {
            if (pluginImpl instanceof Versioned) {
                return ((Versioned) pluginImpl).version();
            }
        } catch (Throwable t) {
            log.error("Failed to get plugin version for " + pluginImpl.getClass(), t);
        }
        return PluginDesc.UNDEFINED_VERSION;
    }

    protected static String reflectiveErrorDescription(Throwable t) {
        if (t instanceof NoSuchMethodException) {
            return ": Plugin class must have a no-args constructor, and cannot be a non-static inner class";
        } else if (t instanceof SecurityException) {
            return ": Security settings must allow reflective instantiation of plugin classes";
        } else if (t instanceof IllegalAccessException) {
            return ": Plugin class default constructor must be public";
        } else if (t instanceof ExceptionInInitializerError) {
            return ": Failed to statically initialize plugin class";
        } else if (t instanceof InvocationTargetException) {
            return ": Failed to invoke plugin constructor";
        } else {
            return "";
        }
    }

    protected LoaderSwap withClassLoader(ClassLoader loader) {
        ClassLoader savedLoader = Plugins.compareAndSwapLoaders(loader);
        try {
            return new LoaderSwap(savedLoader);
        } catch (Throwable t) {
            Plugins.compareAndSwapLoaders(savedLoader);
            throw t;
        }
    }
}
