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
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConnectPluginPath {

    private static final String MANIFEST_PREFIX = "META-INF/services/";
    public static final Object[] LIST_TABLE_COLUMNS = {
        "pluginName",
        "firstAlias",
        "secondAlias",
        "pluginVersion",
        "pluginType",
        "isLoadable",
        "hasManifest",
        "pluginLocation" // last because it is least important and most repetitive
    };
    public static final String NO_ALIAS = "N/A";

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
        Set<Path> pluginLocations = new LinkedHashSet<>();
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
            ClassLoader parent = ConnectPluginPath.class.getClassLoader();
            ServiceLoaderScanner serviceLoaderScanner = new ServiceLoaderScanner();
            ReflectionScanner reflectionScanner = new ReflectionScanner();
            // Process the contents of the classpath to exclude it from later results.
            PluginSource classpathSource = PluginUtils.classpathPluginSource(parent);
            Map<String, List<ManifestEntry>> classpathManifests = findManifests(classpathSource, Collections.emptyMap());
            PluginScanResult classpathPlugins = discoverPlugins(classpathSource, reflectionScanner, serviceLoaderScanner);
            Map<Path, Set<Row>> rowsByLocation = new LinkedHashMap<>();
            Set<Row> classpathRows = enumerateRows(null, classpathManifests, classpathPlugins);
            rowsByLocation.put(null, classpathRows);

            ClassLoaderFactory factory = new ClassLoaderFactory();
            try (DelegatingClassLoader delegatingClassLoader = factory.newDelegatingClassLoader(parent)) {
                beginCommand(config);
                for (Path pluginLocation : config.locations) {
                    PluginSource source = PluginUtils.isolatedPluginSource(pluginLocation, delegatingClassLoader, factory);
                    Map<String, List<ManifestEntry>> manifests = findManifests(source, classpathManifests);
                    PluginScanResult plugins = discoverPlugins(source, reflectionScanner, serviceLoaderScanner);
                    Set<Row> rows = enumerateRows(pluginLocation, manifests, plugins);
                    rowsByLocation.put(pluginLocation, rows);
                    for (Row row : rows) {
                        handlePlugin(config, row);
                    }
                }
                endCommand(config, rowsByLocation);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * The unit of work for a command.
     * <p>This is unique to the (source, class, type) tuple, and contains additional pre-computed information
     * that pertains to this specific plugin.
     */
    private static class Row {
        private final Path pluginLocation;
        private final String className;
        private final PluginType type;
        private final String version;
        private final List<String> aliases;
        private final boolean loadable;
        private final boolean hasManifest;

        public Row(Path pluginLocation, String className, PluginType type, String version, List<String> aliases, boolean loadable, boolean hasManifest) {
            this.pluginLocation = pluginLocation;
            this.className = Objects.requireNonNull(className, "className must be non-null");
            this.version = Objects.requireNonNull(version, "version must be non-null");
            this.type = Objects.requireNonNull(type, "type must be non-null");
            this.aliases = Objects.requireNonNull(aliases, "aliases must be non-null");
            this.loadable = loadable;
            this.hasManifest = hasManifest;
        }

        private boolean loadable() {
            return loadable;
        }

        private boolean compatible() {
            return loadable && hasManifest;
        }

        private boolean incompatible() {
            return !compatible();
        }

        private String locationString() {
            return pluginLocation == null ? "classpath" : pluginLocation.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Row row = (Row) o;
            return Objects.equals(pluginLocation, row.pluginLocation) && className.equals(row.className) && type == row.type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pluginLocation, className, type);
        }
    }

    private static Set<Row> enumerateRows(Path pluginLocation, Map<String, List<ManifestEntry>> manifests, PluginScanResult scanResult) {
        Set<Row> rows = new HashSet<>();
        // Perform a deep copy of the manifests because we're going to be mutating our copy.
        Map<String, Set<ManifestEntry>> unloadablePlugins = manifests.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new HashSet<>(e.getValue())));
        scanResult.forEach(pluginDesc -> {
            // Emit a loadable row for this scan result, since it was found during plugin discovery
            Set<String> rowAliases = new LinkedHashSet<>();
            rowAliases.add(PluginUtils.simpleName(pluginDesc));
            rowAliases.add(PluginUtils.prunedName(pluginDesc));
            rows.add(newRow(pluginLocation, pluginDesc.className(), new ArrayList<>(rowAliases), pluginDesc.type(), pluginDesc.version(), true, manifests));
            // Remove the ManifestEntry if it has the same className and type as one of the loadable plugins.
            unloadablePlugins.getOrDefault(pluginDesc.className(), Collections.emptySet()).removeIf(entry -> entry.type == pluginDesc.type());
        });
        unloadablePlugins.values().forEach(entries -> entries.forEach(entry -> {
            // Emit a non-loadable row, since all the loadable rows showed up in the previous iteration.
            // Two ManifestEntries may produce the same row if they have different URIs
            rows.add(newRow(pluginLocation, entry.className, Collections.emptyList(), entry.type, PluginDesc.UNDEFINED_VERSION, false, manifests));
        }));
        return rows;
    }

    private static Row newRow(Path pluginLocation, String className, List<String> rowAliases, PluginType type, String version, boolean loadable, Map<String, List<ManifestEntry>> manifests) {
        boolean hasManifest = manifests.containsKey(className) && manifests.get(className).stream().anyMatch(e -> e.type == type);
        return new Row(pluginLocation, className, type, version, rowAliases, loadable, hasManifest);
    }

    private static void beginCommand(Config config) {
        if (config.command == Command.LIST) {
            // The list command prints a TSV-formatted table with details of the found plugins
            // This is officially human-readable output with no guarantees for backwards-compatibility
            // It should be reasonably easy to parse for ad-hoc scripting use-cases.
            listTablePrint(config, LIST_TABLE_COLUMNS);
        }
    }

    private static void handlePlugin(Config config, Row row) {
        if (config.command == Command.LIST) {
            String firstAlias = row.aliases.size() > 0 ? row.aliases.get(0) : NO_ALIAS;
            String secondAlias = row.aliases.size() > 1 ? row.aliases.get(1) : NO_ALIAS;
            listTablePrint(config,
                    row.className,
                    firstAlias,
                    secondAlias,
                    row.version,
                    row.type,
                    row.loadable,
                    row.hasManifest,
                    // last because it is least important and most repetitive
                    row.locationString()
            );
        }
    }

    private static void endCommand(
            Config config,
            Map<Path, Set<Row>> rowsByLocation
    ) {
        if (config.command == Command.LIST) {
            // end the table with an empty line to enable users to separate the table from the summary.
            config.out.println();
            rowsByLocation.remove(null);
            Set<Row> isolatedRows = rowsByLocation.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
            long totalPlugins = isolatedRows.size();
            long loadablePlugins = isolatedRows.stream().filter(Row::loadable).count();
            long compatiblePlugins = isolatedRows.stream().filter(Row::compatible).count();
            config.out.printf("Total plugins:      \t%d%n", totalPlugins);
            config.out.printf("Loadable plugins:   \t%d%n", loadablePlugins);
            config.out.printf("Compatible plugins: \t%d%n", compatiblePlugins);
        }
    }

    private static void listTablePrint(Config config, Object... args) {
        if (ConnectPluginPath.LIST_TABLE_COLUMNS.length != args.length) {
            throw new IllegalArgumentException("Table must have exactly " + ConnectPluginPath.LIST_TABLE_COLUMNS.length + " columns");
        }
        config.out.println(Stream.of(args)
                .map(Objects::toString)
                .collect(Collectors.joining("\t")));
    }

    private static PluginScanResult discoverPlugins(PluginSource source, ReflectionScanner reflectionScanner, ServiceLoaderScanner serviceLoaderScanner) {
        PluginScanResult serviceLoadResult = serviceLoaderScanner.discoverPlugins(Collections.singleton(source));
        PluginScanResult reflectiveResult = reflectionScanner.discoverPlugins(Collections.singleton(source));
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

    private static Map<String, List<ManifestEntry>> findManifests(PluginSource source, Map<String, List<ManifestEntry>> exclude) {
        Map<String, List<ManifestEntry>> manifests = new LinkedHashMap<>();
        for (PluginType type : PluginType.values()) {
            try {
                Enumeration<URL> resources = source.loader().getResources(MANIFEST_PREFIX + type.superClass().getName());
                while (resources.hasMoreElements())  {
                    URL url = resources.nextElement();
                    for (String className : parse(url)) {
                        ManifestEntry e = new ManifestEntry(url.toURI(), className, type);
                        manifests.computeIfAbsent(className, ignored -> new ArrayList<>()).add(e);
                    }
                }
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        for (Map.Entry<String, List<ManifestEntry>> entry : exclude.entrySet()) {
            String className = entry.getKey();
            List<ManifestEntry> excluded = entry.getValue();
            // Note this must be a remove and not removeAll, because we want to remove only one copy at a time.
            // If the same jar is present on the classpath and plugin path, then manifests will contain 2 identical
            // ManifestEntry instances, with a third copy in the excludes. After the excludes are processed,
            // manifests should contain exactly one copy of the ManifestEntry.
            for (ManifestEntry e : excluded) {
                manifests.getOrDefault(className, Collections.emptyList()).remove(e);
            }
        }
        return manifests;
    }

    // Based on implementation from ServiceLoader.LazyClassPathLookupIterator from OpenJDK11
    private static Set<String> parse(URL u) {
        Set<String> names = new LinkedHashSet<>(); // preserve insertion order
        try {
            URLConnection uc = u.openConnection();
            uc.setUseCaches(false);
            try (InputStream in = uc.getInputStream();
                 BufferedReader r
                         = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                int lc = 1;
                while ((lc = parseLine(u, r, lc, names)) >= 0) {
                    // pass
                }
            }
        } catch (IOException x) {
            throw new RuntimeException("Error accessing configuration file", x);
        }
        return names;
    }

    // Based on implementation from ServiceLoader.LazyClassPathLookupIterator from OpenJDK11
    private static int parseLine(URL u, BufferedReader r, int lc, Set<String> names) throws IOException {
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
                throw new IOException("Illegal configuration-file syntax in " + u);
            int cp = ln.codePointAt(0);
            if (!Character.isJavaIdentifierStart(cp))
                throw new IOException("Illegal provider-class name: " + ln + " in " + u);
            int start = Character.charCount(cp);
            for (int i = start; i < n; i += Character.charCount(cp)) {
                cp = ln.codePointAt(i);
                if (!Character.isJavaIdentifierPart(cp) && (cp != '.'))
                    throw new IOException("Illegal provider-class name: " + ln + " in " + u);
            }
            names.add(ln);
        }
        return lc + 1;
    }
}
