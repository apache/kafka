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

import org.reflections.util.ClasspathHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Connect plugin utility methods.
 */
public class PluginUtils {
    private static final Logger log = LoggerFactory.getLogger(PluginUtils.class);

    // Be specific about javax packages and exclude those existing in Java SE and Java EE libraries.
    private static final Pattern EXCLUDE = Pattern.compile("^(?:"
            + "java"
            + "|javax\\.accessibility"
            + "|javax\\.activation"
            + "|javax\\.activity"
            + "|javax\\.annotation"
            + "|javax\\.batch\\.api"
            + "|javax\\.batch\\.operations"
            + "|javax\\.batch\\.runtime"
            + "|javax\\.crypto"
            + "|javax\\.decorator"
            + "|javax\\.ejb"
            + "|javax\\.el"
            + "|javax\\.enterprise\\.concurrent"
            + "|javax\\.enterprise\\.context"
            + "|javax\\.enterprise\\.context\\.spi"
            + "|javax\\.enterprise\\.deploy\\.model"
            + "|javax\\.enterprise\\.deploy\\.shared"
            + "|javax\\.enterprise\\.deploy\\.spi"
            + "|javax\\.enterprise\\.event"
            + "|javax\\.enterprise\\.inject"
            + "|javax\\.enterprise\\.inject\\.spi"
            + "|javax\\.enterprise\\.util"
            + "|javax\\.faces"
            + "|javax\\.imageio"
            + "|javax\\.inject"
            + "|javax\\.interceptor"
            + "|javax\\.jms"
            + "|javax\\.json"
            + "|javax\\.jws"
            + "|javax\\.lang\\.model"
            + "|javax\\.mail"
            + "|javax\\.management"
            + "|javax\\.management\\.j2ee"
            + "|javax\\.naming"
            + "|javax\\.net"
            + "|javax\\.persistence"
            + "|javax\\.print"
            + "|javax\\.resource"
            + "|javax\\.rmi"
            + "|javax\\.script"
            + "|javax\\.security\\.auth"
            + "|javax\\.security\\.auth\\.message"
            + "|javax\\.security\\.cert"
            + "|javax\\.security\\.jacc"
            + "|javax\\.security\\.sasl"
            + "|javax\\.servlet"
            + "|javax\\.sound\\.midi"
            + "|javax\\.sound\\.sampled"
            + "|javax\\.sql"
            + "|javax\\.swing"
            + "|javax\\.tools"
            + "|javax\\.transaction"
            + "|javax\\.validation"
            + "|javax\\.websocket"
            + "|javax\\.ws\\.rs"
            + "|javax\\.xml"
            + "|javax\\.xml\\.bind"
            + "|javax\\.xml\\.registry"
            + "|javax\\.xml\\.rpc"
            + "|javax\\.xml\\.soap"
            + "|javax\\.xml\\.ws"
            + "|org\\.ietf\\.jgss"
            + "|org\\.omg\\.CORBA"
            + "|org\\.omg\\.CosNaming"
            + "|org\\.omg\\.Dynamic"
            + "|org\\.omg\\.DynamicAny"
            + "|org\\.omg\\.IOP"
            + "|org\\.omg\\.Messaging"
            + "|org\\.omg\\.PortableInterceptor"
            + "|org\\.omg\\.PortableServer"
            + "|org\\.omg\\.SendingContext"
            + "|org\\.omg\\.stub\\.java\\.rmi"
            + "|org\\.w3c\\.dom"
            + "|org\\.xml\\.sax"
            + "|org\\.apache\\.kafka"
            + "|org\\.slf4j"
            + ")\\..*$");

    // If the base interface or class that will be used to identify Connect plugins resides within
    // the same java package as the plugins that need to be loaded in isolation (and thus are
    // added to the INCLUDE pattern), then this base interface or class needs to be excluded in the
    // regular expression pattern
    private static final Pattern INCLUDE = Pattern.compile("^org\\.apache\\.kafka\\.(?:connect\\.(?:"
            + "transforms\\.(?!Transformation|predicates\\.Predicate$).*"
            + "|json\\..*"
            + "|file\\..*"
            + "|mirror\\..*"
            + "|mirror-client\\..*"
            + "|converters\\..*"
            + "|storage\\.StringConverter"
            + "|storage\\.SimpleHeaderConverter"
            + "|rest\\.basic\\.auth\\.extension\\.BasicAuthSecurityRestExtension"
            + "|connector\\.policy\\.(?!ConnectorClientConfig(?:OverridePolicy|Request(?:\\$ClientType)?)$).*"
            + ")"
            + "|common\\.config\\.provider\\.(?!ConfigProvider$).*"
            + ")$");

    private static final Pattern COMMA_WITH_WHITESPACE = Pattern.compile("\\s*,\\s*");

    private static final DirectoryStream.Filter<Path> PLUGIN_PATH_FILTER = path ->
        Files.isDirectory(path) || isArchive(path) || isClassFile(path);

    /**
     * Return whether the class with the given name should be loaded in isolation using a plugin
     * classloader.
     *
     * @param name the fully qualified name of the class.
     * @return true if this class should be loaded in isolation, false otherwise.
     */
    public static boolean shouldLoadInIsolation(String name) {
        return !(EXCLUDE.matcher(name).matches() && !INCLUDE.matcher(name).matches());
    }

    /**
     * Verify the given class corresponds to a concrete class and not to an abstract class or
     * interface.
     * @param klass the class object.
     * @return true if the argument is a concrete class, false if it's abstract or interface.
     */
    public static boolean isConcrete(Class<?> klass) {
        int mod = klass.getModifiers();
        return !Modifier.isAbstract(mod) && !Modifier.isInterface(mod);
    }

    /**
     * Return whether a path corresponds to a JAR or ZIP archive.
     *
     * @param path the path to validate.
     * @return true if the path is a JAR or ZIP archive file, otherwise false.
     */
    public static boolean isArchive(Path path) {
        String archivePath = path.toString().toLowerCase(Locale.ROOT);
        return archivePath.endsWith(".jar") || archivePath.endsWith(".zip");
    }

    /**
     * Return whether a path corresponds java class file.
     *
     * @param path the path to validate.
     * @return true if the path is a java class file, otherwise false.
     */
    public static boolean isClassFile(Path path) {
        return path.toString().toLowerCase(Locale.ROOT).endsWith(".class");
    }

    public static Set<Path> pluginLocations(String pluginPath, boolean failFast) {
        if (pluginPath == null) {
            return Collections.emptySet();
        }
        String[] pluginPathElements = COMMA_WITH_WHITESPACE.split(pluginPath.trim(), -1);
        Set<Path> pluginLocations = new LinkedHashSet<>();
        for (String path : pluginPathElements) {
            try {
                Path pluginPathElement = Paths.get(path).toAbsolutePath();
                if (pluginPath.isEmpty()) {
                    log.warn("Plugin path element is empty, evaluating to {}.", pluginPathElement);
                }
                if (!Files.exists(pluginPathElement)) {
                    throw new FileNotFoundException(pluginPathElement.toString());
                }
                // Currently 'plugin.paths' property is a list of top-level directories
                // containing plugins
                if (Files.isDirectory(pluginPathElement)) {
                    pluginLocations.addAll(pluginLocations(pluginPathElement));
                } else if (isArchive(pluginPathElement)) {
                    pluginLocations.add(pluginPathElement);
                }
            } catch (InvalidPathException | IOException e) {
                if (failFast) {
                    throw new RuntimeException(e);
                }
                log.error("Could not get listing for plugin path: {}. Ignoring.", path, e);
            }
        }
        return pluginLocations;
    }

    private static List<Path> pluginLocations(Path pluginPathElement) throws IOException {
        List<Path> locations = new ArrayList<>();
        try (
                DirectoryStream<Path> listing = Files.newDirectoryStream(
                        pluginPathElement,
                        PLUGIN_PATH_FILTER
                )
        ) {
            for (Path dir : listing) {
                locations.add(dir);
            }
        }
        return locations;
    }

    /**
     * Given a top path in the filesystem, return a list of paths to archives (JAR or ZIP
     * files) contained under this top path. If the top path contains only java class files,
     * return the top path itself. This method follows symbolic links to discover archives and
     * returns the such archives as absolute paths.
     *
     * @param topPath the path to use as root of plugin search.
     * @return a list of potential plugin paths, or empty list if no such paths exist.
     * @throws IOException
     */
    public static List<Path> pluginUrls(Path topPath) throws IOException {
        boolean containsClassFiles = false;
        Set<Path> archives = new TreeSet<>();
        LinkedList<DirectoryEntry> dfs = new LinkedList<>();
        Set<Path> visited = new HashSet<>();

        if (isArchive(topPath)) {
            return Collections.singletonList(topPath);
        }

        DirectoryStream<Path> topListing = Files.newDirectoryStream(
                topPath,
                PLUGIN_PATH_FILTER
        );
        dfs.push(new DirectoryEntry(topListing));
        visited.add(topPath);
        try {
            while (!dfs.isEmpty()) {
                Iterator<Path> neighbors = dfs.peek().iterator;
                if (!neighbors.hasNext()) {
                    dfs.pop().stream.close();
                    continue;
                }

                Path adjacent = neighbors.next();
                if (Files.isSymbolicLink(adjacent)) {
                    try {
                        Path symlink = Files.readSymbolicLink(adjacent);
                        // if symlink is absolute resolve() returns the absolute symlink itself
                        Path parent = adjacent.getParent();
                        if (parent == null) {
                            continue;
                        }
                        Path absolute = parent.resolve(symlink).toRealPath();
                        if (Files.exists(absolute)) {
                            adjacent = absolute;
                        } else {
                            continue;
                        }
                    } catch (IOException e) {
                        // See https://issues.apache.org/jira/browse/KAFKA-6288 for a reported
                        // failure. Such a failure at this stage is not easily reproducible and
                        // therefore an exception is caught and ignored after issuing a
                        // warning. This allows class scanning to continue for non-broken plugins.
                        log.warn(
                                "Resolving symbolic link '{}' failed. Ignoring this path.",
                                adjacent,
                                e
                        );
                        continue;
                    }
                }

                if (!visited.contains(adjacent)) {
                    visited.add(adjacent);
                    if (isArchive(adjacent)) {
                        archives.add(adjacent);
                    } else if (isClassFile(adjacent)) {
                        containsClassFiles = true;
                    } else {
                        DirectoryStream<Path> listing = Files.newDirectoryStream(
                                adjacent,
                                PLUGIN_PATH_FILTER
                        );
                        dfs.push(new DirectoryEntry(listing));
                    }
                }
            }
        } finally {
            while (!dfs.isEmpty()) {
                dfs.pop().stream.close();
            }
        }

        if (containsClassFiles) {
            if (archives.isEmpty()) {
                return Collections.singletonList(topPath);
            }
            log.warn("Plugin path contains both java archives and class files. Returning only the"
                    + " archives");
        }
        return Arrays.asList(archives.toArray(new Path[0]));
    }

    public static Set<PluginSource> pluginSources(Set<Path> pluginLocations, ClassLoader classLoader, PluginClassLoaderFactory factory) {
        Set<PluginSource> pluginSources = new LinkedHashSet<>();
        for (Path pluginLocation : pluginLocations) {
            try {
                pluginSources.add(isolatedPluginSource(pluginLocation, classLoader, factory));
            } catch (InvalidPathException | MalformedURLException e) {
                log.error("Invalid path in plugin path: {}. Ignoring.", pluginLocation, e);
            } catch (IOException e) {
                log.error("Could not get listing for plugin path: {}. Ignoring.", pluginLocation, e);
            }
        }
        pluginSources.add(classpathPluginSource(classLoader.getParent()));
        return pluginSources;
    }

    public static PluginSource isolatedPluginSource(Path pluginLocation, ClassLoader parent, PluginClassLoaderFactory factory) throws IOException {
        List<URL> pluginUrls = new ArrayList<>();
        List<Path> paths = pluginUrls(pluginLocation);
        // Infer the type of the source
        PluginSource.Type type;
        if (paths.size() == 1 && paths.get(0) == pluginLocation) {
            if (PluginUtils.isArchive(pluginLocation)) {
                type = PluginSource.Type.SINGLE_JAR;
            } else {
                type = PluginSource.Type.CLASS_HIERARCHY;
            }
        } else {
            type = PluginSource.Type.MULTI_JAR;
        }
        for (Path path : paths) {
            pluginUrls.add(path.toUri().toURL());
        }
        URL[] urls = pluginUrls.toArray(new URL[0]);
        PluginClassLoader loader = factory.newPluginClassLoader(
                pluginLocation.toUri().toURL(),
                urls,
                parent
        );
        return new PluginSource(pluginLocation, type, loader, urls);
    }

    public static PluginSource classpathPluginSource(ClassLoader classLoader) {
        List<URL> parentUrls = new ArrayList<>();
        parentUrls.addAll(ClasspathHelper.forJavaClassPath());
        parentUrls.addAll(ClasspathHelper.forClassLoader(classLoader));
        return new PluginSource(null, PluginSource.Type.CLASSPATH, classLoader, parentUrls.toArray(new URL[0]));
    }

    /**
     * Return the simple class name of a plugin as {@code String}.
     *
     * @param plugin the plugin descriptor.
     * @return the plugin's simple class name.
     */
    public static String simpleName(PluginDesc<?> plugin) {
        return plugin.pluginClass().getSimpleName();
    }

    /**
     * Remove the plugin type name at the end of a plugin class name, if such suffix is present.
     * This method is meant to be used to extract plugin aliases.
     *
     * @param plugin the plugin descriptor.
     * @return the pruned simple class name of the plugin.
     */
    public static String prunedName(PluginDesc<?> plugin) {
        // It's currently simpler to switch on type than do pattern matching.
        switch (plugin.type()) {
            case SOURCE:
            case SINK:
                return prunePluginName(plugin, "Connector");
            default:
                return prunePluginName(plugin, plugin.type().simpleName());
        }
    }

    private static String prunePluginName(PluginDesc<?> plugin, String suffix) {
        String simple = plugin.pluginClass().getSimpleName();
        int pos = simple.lastIndexOf(suffix);
        if (pos > 0) {
            return simple.substring(0, pos);
        }
        return simple;
    }

    public static Map<String, String> computeAliases(PluginScanResult scanResult) {
        Map<String, Set<String>> aliasCollisions = new HashMap<>();
        scanResult.forEach(pluginDesc -> {
            aliasCollisions.computeIfAbsent(simpleName(pluginDesc), ignored -> new HashSet<>()).add(pluginDesc.className());
            aliasCollisions.computeIfAbsent(prunedName(pluginDesc), ignored -> new HashSet<>()).add(pluginDesc.className());
        });
        Map<String, String> aliases = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : aliasCollisions.entrySet()) {
            String alias = entry.getKey();
            Set<String> classNames = entry.getValue();
            if (classNames.size() == 1) {
                aliases.put(alias, classNames.stream().findAny().get());
            } else {
                log.debug("Ignoring ambiguous alias '{}' since it refers to multiple distinct plugins {}", alias, classNames);
            }
        }
        return aliases;
    }

    private static class DirectoryEntry {
        final DirectoryStream<Path> stream;
        final Iterator<Path> iterator;

        DirectoryEntry(DirectoryStream<Path> stream) {
            this.stream = stream;
            this.iterator = stream.iterator();
        }
    }

}
