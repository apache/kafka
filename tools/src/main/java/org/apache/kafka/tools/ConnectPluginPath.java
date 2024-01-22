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

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
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
            Config config = parseConfig(parser, namespace, out, err);
            runCommand(config);
            return 0;
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            return 1;
        } catch (TerseException e) {
            err.println(e.getMessage());
            return 2;
        } catch (Throwable e) {
            err.println(Utils.stackTrace(e));
            err.println(e.getMessage());
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

        ArgumentParser syncManifestsCommand = parser.addSubparsers()
            .description("Mutate the specified plugins to be compatible with plugin.discovery=SERVICE_LOAD mode")
            .dest("subcommand")
            .addParser("sync-manifests");

        ArgumentParser[] subparsers = new ArgumentParser[] {
            listCommand,
            syncManifestsCommand
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

        syncManifestsCommand.addArgument("--dry-run")
            .action(Arguments.storeTrue())
            .help("If specified, changes that would have been written to disk are not applied");

        syncManifestsCommand.addArgument("--keep-not-found")
            .action(Arguments.storeTrue())
            .help("If specified, manifests for missing plugins are not removed from the plugin path");

        return parser;
    }

    private static Config parseConfig(ArgumentParser parser, Namespace namespace, PrintStream out, PrintStream err) throws ArgumentParserException, TerseException {
        Set<Path> locations = parseLocations(parser, namespace);
        String subcommand = namespace.getString("subcommand");
        if (subcommand == null) {
            throw new ArgumentParserException("No subcommand specified", parser);
        }
        switch (subcommand) {
            case "list":
                return new Config(Command.LIST, locations, false, false, out, err);
            case "sync-manifests":
                return new Config(Command.SYNC_MANIFESTS, locations, namespace.getBoolean("dry_run"), namespace.getBoolean("keep_not_found"), out, err);
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
        LIST, SYNC_MANIFESTS;
    }

    private static class Config {
        private final Command command;
        private final Set<Path> locations;
        private final boolean dryRun;
        private final boolean keepNotFound;
        private final PrintStream out;
        private final PrintStream err;

        private Config(Command command, Set<Path> locations, boolean dryRun, boolean keepNotFound, PrintStream out, PrintStream err) {
            this.command = command;
            this.locations = locations;
            this.dryRun = dryRun;
            this.keepNotFound = keepNotFound;
            this.out = out;
            this.err = err;
        }

        @Override
        public String toString() {
            return "Config{" +
                "command=" + command +
                ", locations=" + locations +
                ", dryRun=" + dryRun +
                ", keepNotFound=" + keepNotFound +
                '}';
        }
    }

    public static void runCommand(Config config) throws TerseException {
        try {
            ManifestWorkspace workspace = new ManifestWorkspace(config.out);
            ClassLoader parent = ConnectPluginPath.class.getClassLoader();
            ServiceLoaderScanner serviceLoaderScanner = new ServiceLoaderScanner();
            ReflectionScanner reflectionScanner = new ReflectionScanner();
            PluginSource classpathSource = PluginUtils.classpathPluginSource(parent);
            ManifestWorkspace.SourceWorkspace<?> classpathWorkspace = workspace.forSource(classpathSource);
            PluginScanResult classpathPlugins = discoverPlugins(classpathSource, reflectionScanner, serviceLoaderScanner);
            Map<Path, Set<Row>> rowsByLocation = new LinkedHashMap<>();
            Set<Row> classpathRows = enumerateRows(classpathWorkspace, classpathPlugins);
            rowsByLocation.put(null, classpathRows);

            ClassLoaderFactory factory = new ClassLoaderFactory();
            try (DelegatingClassLoader delegatingClassLoader = factory.newDelegatingClassLoader(parent)) {
                beginCommand(config);
                for (Path pluginLocation : config.locations) {
                    PluginSource source = PluginUtils.isolatedPluginSource(pluginLocation, delegatingClassLoader, factory);
                    ManifestWorkspace.SourceWorkspace<?> pluginWorkspace = workspace.forSource(source);
                    PluginScanResult plugins = discoverPlugins(source, reflectionScanner, serviceLoaderScanner);
                    Set<Row> rows = enumerateRows(pluginWorkspace, plugins);
                    rowsByLocation.put(pluginLocation, rows);
                    for (Row row : rows) {
                        handlePlugin(config, row);
                    }
                }
                endCommand(config, workspace, rowsByLocation);
            }
        } catch (Throwable e) {
            failCommand(config, e);
        }
    }

    /**
     * The unit of work for a command.
     * <p>This is unique to the (source, class, type) tuple, and contains additional pre-computed information
     * that pertains to this specific plugin.
     */
    private static class Row {
        private final ManifestWorkspace.SourceWorkspace<?> workspace;
        private final String className;
        private final PluginType type;
        private final String version;
        private final List<String> aliases;
        private final boolean loadable;
        private final boolean hasManifest;

        public Row(ManifestWorkspace.SourceWorkspace<?> workspace, String className, PluginType type, String version, List<String> aliases, boolean loadable, boolean hasManifest) {
            this.workspace = Objects.requireNonNull(workspace, "workspace must be non-null");
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

        private String locationString() {
            Path pluginLocation = workspace.location();
            return pluginLocation == null ? "classpath" : pluginLocation.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Row row = (Row) o;
            return Objects.equals(workspace, row.workspace) && className.equals(row.className) && type == row.type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(workspace, className, type);
        }
    }

    private static Set<Row> enumerateRows(ManifestWorkspace.SourceWorkspace<?> workspace, PluginScanResult scanResult) {
        Set<Row> rows = new HashSet<>();
        Map<String, Set<PluginType>> nonLoadableManifests = new HashMap<>();
        workspace.forEach((className, type) -> {
            // Mark all manifests in the workspace as non-loadable first
            nonLoadableManifests.computeIfAbsent(className, ignored -> EnumSet.of(type)).add(type);
        });
        scanResult.forEach(pluginDesc -> {
            // Only loadable plugins appear in the scan result
            Set<String> rowAliases = new LinkedHashSet<>();
            rowAliases.add(PluginUtils.simpleName(pluginDesc));
            rowAliases.add(PluginUtils.prunedName(pluginDesc));
            rows.add(newRow(workspace, pluginDesc.className(), new ArrayList<>(rowAliases), pluginDesc.type(), pluginDesc.version(), true));
            // If a corresponding manifest exists, mark it as loadable by removing it from the map.
            nonLoadableManifests.getOrDefault(pluginDesc.className(), Collections.emptySet()).remove(pluginDesc.type());
        });
        nonLoadableManifests.forEach((className, types) -> types.forEach(type -> {
            // All manifests which remain in the map are not loadable
            rows.add(newRow(workspace, className, Collections.emptyList(), type, PluginDesc.UNDEFINED_VERSION, false));
        }));
        return rows;
    }

    private static Row newRow(ManifestWorkspace.SourceWorkspace<?> workspace, String className, List<String> rowAliases, PluginType type, String version, boolean loadable) {
        boolean hasManifest = workspace.hasManifest(type, className);
        return new Row(workspace, className, type, version, rowAliases, loadable, hasManifest);
    }

    private static void beginCommand(Config config) {
        if (config.command == Command.LIST) {
            // The list command prints a TSV-formatted table with details of the found plugins
            // This is officially human-readable output with no guarantees for backwards-compatibility
            // It should be reasonably easy to parse for ad-hoc scripting use-cases.
            listTablePrint(config, LIST_TABLE_COLUMNS);
        } else if (config.command == Command.SYNC_MANIFESTS) {
            if (config.dryRun) {
                config.out.println("Dry run started: No changes will be committed.");
            }
            config.out.println("Scanning for plugins...");
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
        } else if (config.command == Command.SYNC_MANIFESTS) {
            if (row.loadable && !row.hasManifest) {
                row.workspace.addManifest(row.type, row.className);
            } else if (!row.loadable && row.hasManifest && !config.keepNotFound) {
                row.workspace.removeManifest(row.type, row.className);
            }
        }
    }

    private static void endCommand(
            Config config,
            ManifestWorkspace workspace,
            Map<Path, Set<Row>> rowsByLocation
    ) throws IOException, TerseException {
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
        } else if (config.command == Command.SYNC_MANIFESTS) {
            if (workspace.commit(true)) {
                if (config.dryRun) {
                    config.out.println("Dry run passed: All above changes can be committed to disk if re-run with dry run disabled.");
                } else {
                    config.out.println("Writing changes to plugins...");
                    try {
                        workspace.commit(false);
                    } catch (Throwable t) {
                        config.err.println(Utils.stackTrace(t));
                        throw new TerseException("Sync incomplete due to exception; plugin path may be corrupted. Discard the contents of the plugin.path before retrying.");
                    }
                    config.out.println("All loadable plugins have accurate ServiceLoader manifests.");
                }
            } else {
                config.out.println("No changes required.");
            }
        }
    }

    private static void failCommand(Config config, Throwable e) throws TerseException {
        if (e instanceof TerseException) {
            throw (TerseException) e;
        }
        if (config.command == Command.LIST) {
            throw new RuntimeException("Unexpected error occurred while listing plugins", e);
        } else if (config.command == Command.SYNC_MANIFESTS) {
            // The real write errors are propagated as a TerseException, and don't take this branch.
            throw new RuntimeException("Unexpected error occurred while dry-running sync", e);
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
}
