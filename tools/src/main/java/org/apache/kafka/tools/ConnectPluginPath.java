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
package org.apache.kafka.tools;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.ClassLoaderFactory;
import org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.PluginScanResult;
import org.apache.kafka.connect.runtime.isolation.PluginSource;
import org.apache.kafka.connect.runtime.isolation.PluginType;
import org.apache.kafka.connect.runtime.isolation.PluginUtils;
import org.apache.kafka.connect.runtime.isolation.ReflectionScanner;
import org.apache.kafka.connect.runtime.isolation.ServiceLoaderScanner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConnectPluginPath {

    private static final String MANIFEST_PREFIX = "META-INF/services/";

    public static void main(String[] args) {
        Exit.exit(mainNoExit(args, System.out, System.err));
    }

    public static int mainNoExit(String[] args, PrintStream out, PrintStream err) {
        ArgumentParser parser = parser();
        try {
            Namespace namespace = parser.parseArgs(args);
            Config config = parseConfig(parser, namespace, out);
            runCommand(config);
            return 0;
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            return 1;
        } catch (TerseException e) {
            err.println(e.getMessage());
            return 2;
        } catch (Throwable e) {
            err.println(e.getMessage());
            err.println(Utils.stackTrace(e));
            return 3;
        }
    }

    private static ArgumentParser parser() {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("connect-plugin-path")
            .defaultHelp(true)
            .description("Manage plugins on the Connect plugin.path");

        ArgumentParser listCommand = parser.addSubparsers()
            .description("List information about plugins contained within the specified plugin locations")
            .dest("subcommand")
            .addParser("list");

        ArgumentParser[] subparsers = new ArgumentParser[] {
            listCommand,
        };

        for (ArgumentParser subparser : subparsers) {
            ArgumentGroup pluginProviders = subparser.addArgumentGroup("plugin providers");
            pluginProviders.addArgument("--plugin-location")
                .setDefault(new ArrayList<>())
                .action(Arguments.append())
                .help("A single plugin location (jar file or directory)");

            pluginProviders.addArgument("--plugin-path")
                .setDefault(new ArrayList<>())
                .action(Arguments.append())
                .help("A comma-delimited list of locations containing plugins");

            pluginProviders.addArgument("--worker-config")
                .setDefault(new ArrayList<>())
                .action(Arguments.append())
                .help("A Connect worker configuration file");
        }

        return parser;
    }

    private static Config parseConfig(ArgumentParser parser, Namespace namespace, PrintStream out) throws ArgumentParserException, TerseException {
        Set<Path> locations = parseLocations(parser, namespace);
        String subcommand = namespace.getString("subcommand");
        if (subcommand == null) {
            throw new ArgumentParserException("No subcommand specified", parser);
        }
        switch (subcommand) {
            case "list":
                return new Config(Command.LIST, locations, out);
            default:
                throw new ArgumentParserException("Unrecognized subcommand: '" + subcommand + "'", parser);
        }
    }

    private static Set<Path> parseLocations(ArgumentParser parser, Namespace namespace) throws ArgumentParserException, TerseException {
        List<String> rawLocations = new ArrayList<>(namespace.getList("plugin_location"));
        List<String> rawPluginPaths = new ArrayList<>(namespace.getList("plugin_path"));
        List<String> rawWorkerConfigs = new ArrayList<>(namespace.getList("worker_config"));
        if (rawLocations.isEmpty() && rawPluginPaths.isEmpty() && rawWorkerConfigs.isEmpty()) {
            throw new ArgumentParserException("Must specify at least one --plugin-location, --plugin-path, or --worker-config", parser);
        }
        Set<Path> pluginLocations = new HashSet<>();
        for (String rawWorkerConfig : rawWorkerConfigs) {
            Properties properties;
            try {
                properties = Utils.loadProps(rawWorkerConfig);
            } catch (IOException e) {
                throw new TerseException("Unable to read worker config at " + rawWorkerConfig);
            }
            String pluginPath = properties.getProperty(WorkerConfig.PLUGIN_PATH_CONFIG);
            if (pluginPath != null) {
                rawPluginPaths.add(pluginPath);
            }
        }
        for (String rawPluginPath : rawPluginPaths) {
            try {
                pluginLocations.addAll(PluginUtils.pluginLocations(rawPluginPath, true));
            } catch (UncheckedIOException e) {
                throw new TerseException("Unable to parse plugin path " + rawPluginPath + ": " + e.getMessage());
            }
        }
        for (String rawLocation : rawLocations) {
            Path pluginLocation = Paths.get(rawLocation);
            if (!pluginLocation.toFile().exists()) {
                throw new TerseException("Specified location " + pluginLocation + " does not exist");
            }
            pluginLocations.add(pluginLocation);
        }
        return pluginLocations;
    }

    enum Command {
        LIST
    }

    private static class Config {
        private final Command command;
        private final Set<Path> locations;
        private final PrintStream out;

        private Config(Command command, Set<Path> locations, PrintStream out) {
            this.command = command;
            this.locations = locations;
            this.out = out;
        }

        @Override
        public String toString() {
            return "Config{" +
                "command=" + command +
                ", locations=" + locations +
                '}';
        }
    }

    public static void runCommand(Config config) throws TerseException {
        try {
            // Process the contents of the classpath to exclude it from later results.
            ClassLoader parent = ConnectPluginPath.class.getClassLoader();
            PluginSource classpathSource = PluginUtils.classpathPluginSource(parent);
            Map<String, Set<ManifestEntry>> classpathManifests = findManifests(classpathSource, Collections.emptyMap());
            PluginScanResult classpathPlugins = discoverPlugins(classpathSource);

            ClassLoaderFactory factory = new ClassLoaderFactory();
            try (DelegatingClassLoader delegatingClassLoader = factory.newDelegatingClassLoader(parent)) {
                // Process the contents of the isolated locations to find plugins
                Set<PluginSource> isolatedSources = PluginUtils.isolatedPluginSources(new ArrayList<>(config.locations), delegatingClassLoader, factory);
                // Exclude manifest entries which are visible from the classpath
                Map<PluginSource, Map<String, Set<ManifestEntry>>> isolatedManifests = isolatedSources.stream()
                        .collect(Collectors.toMap(Function.identity(), source -> findManifests(source, classpathManifests)));
                Map<PluginSource, PluginScanResult> isolatedPlugins = isolatedSources.stream()
                        .collect(Collectors.toMap(Function.identity(), ConnectPluginPath::discoverPlugins));
                // Calculate aliases including collisions with classpath plugins
                Map<String, List<String>> aliases = computeAliases(classpathPlugins, isolatedPlugins.values());
                beginCommand(config);
                for (PluginSource isolatedSource : isolatedSources) {
                    handlePluginSource(config, isolatedSource, isolatedManifests.get(isolatedSource), isolatedPlugins.get(isolatedSource), aliases);
                }
                endCommand(config);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void beginCommand(Config config) {

    }

    private static void handlePluginSource(Config config, PluginSource isolatedSource, Map<String, Set<ManifestEntry>> indexedManifests, PluginScanResult merged, Map<String, List<String>> aliases) {
        merged.forEach(pluginDesc -> handlePlugin(config, isolatedSource, aliases, indexedManifests, pluginDesc.className(), pluginDesc.version(), pluginDesc.type(), true));
        Map<String, Set<ManifestEntry>> unloadablePlugins = new HashMap<>(indexedManifests);
        merged.forEach(pluginDesc -> unloadablePlugins.remove(pluginDesc.className()));
        for (Set<ManifestEntry> manifestEntries : unloadablePlugins.values()) {
            manifestEntries.forEach(manifestEntry -> handlePlugin(config, isolatedSource, aliases, indexedManifests, manifestEntry.className, PluginDesc.UNDEFINED_VERSION, manifestEntry.type, false));
        }
    }


    private static void handlePlugin(
            // fixed
            Config config,
            // each location
            PluginSource source,
            Map<String, List<String>> aliases,
            Map<String, Set<ManifestEntry>> manifests,
            // each plugin
            String pluginName,
            String pluginVersion,
            PluginType pluginType,
            boolean loadable
    ) {
        handlePlugin(config, source, pluginName, pluginVersion, aliases.getOrDefault(pluginName, Collections.emptyList()), pluginType, loadable, manifests.containsKey(pluginName));
    }

    private static void handlePlugin(
            // fixed
            Config config,
            // each location
            PluginSource source,
            // each plugin
            String pluginName,
            String pluginVersion,
            List<String> aliases,
            PluginType pluginType,
            boolean loadable,
            boolean hasManifest
    ) {
        Path pluginLocation = source.location();
        if (config.command == Command.LIST) {
            String firstAlias = aliases.size() > 0 ? aliases.get(0) : "null";
            String secondAlias = aliases.size() > 1 ? aliases.get(1) : "null";
            String pluginInfo = Stream.of(
                    pluginName,
                    firstAlias,
                    secondAlias,
                    pluginVersion,
                    pluginType,
                    loadable,
                    hasManifest,
                    pluginLocation // last because it is least important and most repetitive
            ).map(Objects::toString).collect(Collectors.joining("\t"));
            config.out.println(pluginInfo);
        }
    }

    private static void endCommand(Config config) {

    }

    private static PluginScanResult discoverPlugins(PluginSource source) {
        PluginScanResult serviceLoadResult = new ServiceLoaderScanner().discoverPlugins(Collections.singleton(source));
        PluginScanResult reflectiveResult = new ReflectionScanner().discoverPlugins(Collections.singleton(source));
        return new PluginScanResult(Arrays.asList(serviceLoadResult, reflectiveResult));
    }

    private static class ManifestEntry {
        private final URI manifestURI;
        private final String className;
        private final PluginType type;

        private ManifestEntry(URI manifestURI, String className, PluginType type) {
            this.manifestURI = manifestURI;
            this.className = className;
            this.type = type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ManifestEntry that = (ManifestEntry) o;
            return manifestURI.equals(that.manifestURI) && className.equals(that.className) && type == that.type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(manifestURI, className, type);
        }
    }

    private static Map<String, Set<ManifestEntry>> findManifests(PluginSource source, Map<String, Set<ManifestEntry>> exclude) {
        Map<String, Set<ManifestEntry>> manifests = new HashMap<>();
        for (PluginType type : PluginType.values()) {
            if (type == PluginType.UNKNOWN) {
                continue;
            }
            try {
                Enumeration<URL> resources = source.loader().getResources(MANIFEST_PREFIX + type.typeName());
                while (resources.hasMoreElements())  {
                    URL url = resources.nextElement();
                    for (String className : parse(url)) {
                        ManifestEntry e = new ManifestEntry(url.toURI(), className, type);
                        if (!exclude.containsKey(className) || !exclude.get(className).contains(e)) {
                            manifests.computeIfAbsent(className, ignored -> new HashSet<>()).add(e);
                        }
                    }
                }
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return manifests;
    }

    // Based on implementation from ServiceLoader.LazyClassPathLookupIterator
    // visible for testing
    static Set<String> parse(URL u) {
        Set<String> names = new LinkedHashSet<>(); // preserve insertion order
        try {
            URLConnection uc = u.openConnection();
            uc.setUseCaches(false);
            try (InputStream in = uc.getInputStream();
                 BufferedReader r
                         = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                int lc = 1;
                while ((lc = parseLine(r, lc, names)) >= 0) {
                    // pass
                }
            }
        } catch (IOException x) {
            throw new RuntimeException("Error accessing ServiceLoader manifest file " + u, x);
        }
        return names;
    }

    private static int parseLine(BufferedReader r, int lc, Set<String> names) throws IOException {
        String ln = r.readLine();
        if (ln == null) {
            return -1;
        }
        int ci = ln.indexOf('#');
        if (ci >= 0) ln = ln.substring(0, ci);
        ln = ln.trim();
        int n = ln.length();
        if (n != 0) {
            if ((ln.indexOf(' ') >= 0) || (ln.indexOf('\t') >= 0))
                throw new IOException("Illegal configuration-file syntax");
            int cp = ln.codePointAt(0);
            if (!Character.isJavaIdentifierStart(cp))
                throw new IOException("Illegal provider-class name: " + ln);
            int start = Character.charCount(cp);
            for (int i = start; i < n; i += Character.charCount(cp)) {
                cp = ln.codePointAt(i);
                if (!Character.isJavaIdentifierPart(cp) && (cp != '.'))
                    throw new IOException("Illegal provider-class name: " + ln);
            }
            names.add(ln);
        }
        return lc + 1;
    }

    private static Map<String, List<String>> computeAliases(PluginScanResult classpathPlugins, Collection<PluginScanResult> isolatedPlugins) {
        List<PluginScanResult> allResults = new ArrayList<>(isolatedPlugins);
        allResults.add(classpathPlugins);
        return PluginUtils.computeAliases(new PluginScanResult(allResults)).entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getValue,
                        e -> new ArrayList<>(Collections.singletonList(e.getKey())),
                        (a, b) -> {
                            a.addAll(b);
                            return a;
                        }));
    }
}
