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

import org.apache.maven.artifact.versioning.ArtifactVersion;
import org.apache.maven.artifact.versioning.DefaultArtifactVersion;
import org.apache.maven.artifact.versioning.VersionRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * A custom classloader dedicated to loading Connect plugin classes in classloading isolation.
 *
 * <p>
 * Under the current scheme for classloading isolation in Connect, the delegating classloader loads
 * plugin classes that it finds in its child plugin classloaders. For classes that are not plugins,
 * this delegating classloader delegates its loading to its parent. This makes this classloader a
 * child-first classloader.
 * <p>
 * This class is thread-safe and parallel capable.
 */
public class DelegatingClassLoader extends URLClassLoader {
    private static final Logger log = LoggerFactory.getLogger(DelegatingClassLoader.class);

    private final ConcurrentMap<String, SortedMap<PluginDesc<?>, ClassLoader>> pluginLoaders;
    private final ConcurrentMap<String, String> aliases;

    // Although this classloader does not load classes directly but rather delegates loading to a
    // PluginClassLoader or its parent through its base class, because of the use of inheritance
    // in the latter case, this classloader needs to also be declared as parallel capable to use
    // fine-grain locking when loading classes.
    static {
        ClassLoader.registerAsParallelCapable();
    }

    public DelegatingClassLoader(ClassLoader parent) {
        super(new URL[0], parent);
        this.pluginLoaders = new ConcurrentHashMap<>();
        this.aliases = new ConcurrentHashMap<>();
    }

    public DelegatingClassLoader() {
        // Use as parent the classloader that loaded this class. In most cases this will be the
        // System classloader. But this choice here provides additional flexibility in managed
        // environments that control classloading differently (OSGi, Spring and others) and don't
        // depend on the System classloader to load Connect's classes.
        this(DelegatingClassLoader.class.getClassLoader());
    }

    /**
     * Retrieve the PluginClassLoader associated with a plugin class
     *
     * @param name The fully qualified class name of the plugin
     * @return the PluginClassLoader that should be used to load this, or null if the plugin is not isolated.
     */
    // VisibleForTesting
    PluginClassLoader pluginClassLoader(String name, VersionRange range) {
        if (!PluginUtils.shouldLoadInIsolation(name)) {
            return null;
        }

        SortedMap<PluginDesc<?>, ClassLoader> inner = pluginLoaders.get(name);
        if (inner == null) {
            if (range != null) {
                throw new VersionedPluginLoadingException(String.format(
                    "Plugin %s was not found during plugin scan, " +
                        "only plugins discovered with plugin scanners are eligible for explicit version specification", name));
            }
            return null;
        }


        ClassLoader pluginLoader = findPluginLoader(inner, name, range);
        return pluginLoader instanceof PluginClassLoader
            ? (PluginClassLoader) pluginLoader
            : null;
    }

    PluginClassLoader pluginClassLoader(String name) {
        return pluginClassLoader(name, null);
    }

    ClassLoader connectorLoader(String connectorClassOrAlias, VersionRange range) {
        String fullName = aliases.getOrDefault(connectorClassOrAlias, connectorClassOrAlias);
        ClassLoader classLoader = pluginClassLoader(fullName, range);
        if (classLoader == null) classLoader = this;
        log.debug(
            "Getting plugin class loader: '{}' for connector: {}",
            classLoader,
            connectorClassOrAlias
        );
        return classLoader;
    }

    ClassLoader connectorLoader(String connectorClassOrAlias) {
        return connectorLoader(connectorClassOrAlias, null);
    }

    String resolveFullClassName(String classOrAlias) {
        return aliases.getOrDefault(classOrAlias, classOrAlias);
    }

    private ClassLoader findPluginLoader(
        SortedMap<PluginDesc<?>, ClassLoader> loaders,
        String pluginName,
        VersionRange range
    ) {

        if (range != null) {

            ArtifactVersion version = range.getRecommendedVersion();

            if (range.hasRestrictions()) {
                List<ArtifactVersion> versions = loaders.keySet().stream().map(PluginDesc::encodedVersion).collect(Collectors.toList());
                version = range.matchVersion(versions);
                if (version == null) {
                    throw new VersionedPluginLoadingException(String.format(
                        "Plugin loader for %s not found that matches the version range %s, available versions: %s",
                        pluginName,
                        range,
                        versions
                    ));
                }
            }

            if (version != null) {
                for (Map.Entry<PluginDesc<?>, ClassLoader> entry : loaders.entrySet()) {
                    if (entry.getKey().encodedVersion().equals(version)) {
                        return entry.getValue();
                    }
                }
            }
        }

        return loaders.get(loaders.lastKey());
    }

    public void installDiscoveredPlugins(PluginScanResult scanResult) {
        pluginLoaders.putAll(computePluginLoaders(scanResult));
        for (String pluginClassName : pluginLoaders.keySet()) {
            log.info("Added plugin '{}'", pluginClassName);
        }
        aliases.putAll(PluginUtils.computeAliases(scanResult));
        for (Map.Entry<String, String> alias : aliases.entrySet()) {
            log.info("Added alias '{}' to plugin '{}'", alias.getKey(), alias.getValue());
        }
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        String fullName = aliases.getOrDefault(name, name);
        PluginClassLoader pluginLoader = pluginClassLoader(fullName);
        if (pluginLoader != null) {
            log.trace("Retrieving loaded class '{}' from '{}'", fullName, pluginLoader);
            return pluginLoader.loadClass(fullName, resolve);
        }

        return super.loadClass(fullName, resolve);
    }

    protected Class<?> loadVersionedPluginClass(
        String name,
        VersionRange range,
        boolean resolve
    ) throws VersionedPluginLoadingException, ClassNotFoundException {

        String fullName = aliases.getOrDefault(name, name);
        PluginClassLoader pluginLoader = pluginClassLoader(fullName, range);
        Class<?> plugin;
        if (pluginLoader != null) {
            log.trace("Retrieving loaded class '{}' from '{}'", name, pluginLoader);
            plugin = pluginLoader.loadClass(name, resolve);
        } else {
            plugin = super.loadClass(fullName, resolve);
        }

        DefaultArtifactVersion pluginVersion = new DefaultArtifactVersion(PluginScanner.versionFor(plugin));
        if (range != null && range.hasRestrictions() && !range.containsVersion(pluginVersion)) {
            throw new VersionedPluginLoadingException(String.format(
                    "Plugin %s was loaded with version %s which does not match the version range %s",
                    name,
                    pluginVersion,
                    range
            ));
        }
        return plugin;
    }



    private static Map<String, SortedMap<PluginDesc<?>, ClassLoader>> computePluginLoaders(PluginScanResult plugins) {
        Map<String, SortedMap<PluginDesc<?>, ClassLoader>> pluginLoaders = new HashMap<>();
        plugins.forEach(pluginDesc ->
            pluginLoaders.computeIfAbsent(pluginDesc.className(), k -> new TreeMap<>())
                .put(pluginDesc, pluginDesc.loader()));
        return pluginLoaders;
    }
}
