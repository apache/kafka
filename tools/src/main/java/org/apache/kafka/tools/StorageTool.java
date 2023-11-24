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
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.bootstrap.BootstrapDirectory;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.properties.MetaProperties;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.metadata.properties.MetaPropertiesVersion;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.ProcessRole;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.internals.log.LogConfig;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class StorageTool {
    public static void main(String... args) {
        try {
            
            Namespace namespace = parseArguments(args);
            String command = namespace.getString("command");
            Optional<LogConfig> logConfig = Optional.ofNullable(namespace.getString("config")).map(p -> {
                try {
                    return new LogConfig(Utils.loadProps(p));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            executeCommand(namespace, command, logConfig);
            Exit.exit(0);
            
        } catch (TerseException e) {
            System.err.println(e.getMessage());
            Exit.exit(1, e.getMessage());
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            Exit.exit(1, e.getMessage());
        }
    }
    
    private static void executeCommand(Namespace namespace,
                                       String command,
                                       Optional<LogConfig> logConfig) throws Exception {
        switch (command) {
            case "info": {
                List<String> directories = configToLogDirectories(logConfig.get());
                boolean selfManagedMode = configToSelfManagedMode(logConfig.get());
                Exit.exit(infoCommand(System.out, selfManagedMode, directories));
                break;
            }

            case "format": {
                List<String> directories = configToLogDirectories(logConfig.get());
                String clusterId = namespace.getString("cluster_id");
                
                MetadataVersion metadataVersion = getMetadataVersion(namespace, interBrokerProtocolVersion(logConfig.get()));
                if (!metadataVersion.isKRaftSupported()) {
                    throw new TerseException("Must specify a valid KRaft metadata.version of at least " + MetadataVersion.IBP_3_0_IV0 + ".");
                }
                if (!metadataVersion.isProduction()) {
                    if (unstableMetadataVersionsEnabled(logConfig.get())) {
                        System.out.println("WARNING: using pre-production metadata.version " + metadataVersion + ".");
                    } else {
                        throw new TerseException("The metadata.version " + metadataVersion + " is not ready for production use yet.");
                    }
                }
                MetaProperties metaProperties = new MetaProperties.Builder()
                    .setVersion(MetaPropertiesVersion.V1)
                    .setClusterId(clusterId)
                    .setNodeId(nodeId(logConfig.get()))
                    .build();

                List<ApiMessageAndVersion> metadataRecords = new ArrayList<>();
                Optional<List<UserScramCredentialRecord>> scramRecordsOptional = getUserScramCredentialRecords(namespace);
                if (scramRecordsOptional.isPresent()) {
                    if (!metadataVersion.isScramSupported()) {
                        throw new TerseException("SCRAM is only supported in metadata.version " + MetadataVersion.IBP_3_5_IV2 + " or later.");
                    }
                    for (ApiMessage record : scramRecordsOptional.get()) {
                        metadataRecords.add(new ApiMessageAndVersion(record, (short) 0));
                    }
                }

                BootstrapMetadata bootstrapMetadata = buildBootstrapMetadata(metadataVersion, Optional.of(metadataRecords), "format command");
                boolean ignoreFormatted = namespace.getBoolean("ignore_formatted");
                if (!configToSelfManagedMode(logConfig.get())) {
                    throw new TerseException("The kafka configuration file appears to be for " +
                        "a legacy cluster. Formatting is only supported for clusters in KRaft mode.");
                }
                Exit.exit(formatCommand(System.out, directories, metaProperties, 
                    bootstrapMetadata, metadataVersion, ignoreFormatted));
                break;
            }

            case "random-uuid": {
                System.out.println(Uuid.randomUuid());
                Exit.exit(0);
                break;
            }

            default: {
                throw new RuntimeException("Unknown command " + command);
            }
        }
    }
    
    public static Namespace parseArguments(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("kafka-storage", true, "-", "@").description("The Kafka storage tool.");
        Subparsers subparsers = parser.addSubparsers().dest("command");
        Subparser infoParser = subparsers.addParser("info").help("Get information about the Kafka log directories on this node.");
        Subparser formatParser = subparsers.addParser("format").help("Format the Kafka log directories on this node.");
        subparsers.addParser("random-uuid").help("Print a random UUID.");

        for (Subparser subpparser : Arrays.asList(infoParser, formatParser)) {
            subpparser.addArgument("--config", "-c").action(store()).required(true).help("The Kafka configuration file to use.");
        }

        formatParser.addArgument("--cluster-id", "-t").action(store()).required(true).help("The cluster ID to use.");
        formatParser.addArgument("--add-scram", "-S").action(append()).help("A SCRAM_CREDENTIAL to add to the __cluster_metadata log e.g.\n" +
            "'SCRAM-SHA-256=[name=alice,password=alice-secret]'\n" +
            "'SCRAM-SHA-512=[name=alice,iterations=8192,salt=\"N3E=\",saltedpassword=\"YCE=\"]'");
        formatParser.addArgument("--ignore-formatted", "-g").action(storeTrue());
        formatParser.addArgument("--release-version", "-r").action(store()).help(
            "A KRaft release version to use for the initial metadata.version. The minimum is " + MetadataVersion.IBP_3_0_IV0 + 
                " the default is " + MetadataVersion.LATEST_PRODUCTION.version());

        return parser.parseArgsOrFail(args);
    }
    
    static List<String> configToLogDirectories(LogConfig logConfig) {
        List<String> logDirs = logDirs(logConfig);
        SortedSet<String> directories = new TreeSet<>(logDirs);
        String metadataLogDir = metadataLogDir(logConfig);
        if (metadataLogDir != null) {
            directories.add(metadataLogDir);
        }
        return new ArrayList<>(directories);
    }

    private static boolean configToSelfManagedMode(LogConfig logConfig) {
        String processRolesConfig = (String) logConfig.originals().get(KRaftConfigs.PROCESS_ROLES_CONFIG);
        List<String> processRoles = processRolesConfig != null ? Arrays.asList(processRolesConfig.split("\\s*,\\s*")) : Collections.emptyList();
        return !parseProcessRoles(processRoles).isEmpty();
    }

    static MetadataVersion getMetadataVersion(Namespace namespace, Optional<String> defaultVersionString) {
        MetadataVersion defaultValue;
        if (defaultVersionString != null && defaultVersionString.isPresent()) {
            defaultValue = MetadataVersion.fromVersionString(defaultVersionString.get());
        } else {
            defaultValue = MetadataVersion.LATEST_PRODUCTION;
        }
        String releaseVersion = namespace.getString("release_version");
        if (releaseVersion != null) {
            return MetadataVersion.fromVersionString(releaseVersion);
        } else {
            return defaultValue;
        }
    }

    private static UserScramCredentialRecord getUserScramCredentialRecord(String mechanism, String config) 
            throws TerseException, NoSuchAlgorithmException, InvalidKeyException {
        // Remove '[' and ']'
        String cleanedConfig = config.substring(1, config.length() - 1);

        // Split K->V pairs on ',' and no K or V should contain ','
        String[] keyValuePairs = cleanedConfig.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

        // Create Map of K to V and replace all " in V
        Map<String, String> argMap = Arrays.stream(keyValuePairs)
            .map(pair -> pair.split("=", 2))
            .collect(Collectors.toMap(pair -> pair[0], pair -> pair[1].replaceAll("\"", "")));

        ScramMechanism scramMechanism = ScramMechanism.forMechanismName(mechanism);
        String name = getName(argMap);
        byte[] salt = getSalt(argMap, scramMechanism);
        int iterations = getIterations(argMap, scramMechanism);
        byte[] saltedPassword = getSaltedPassword(argMap, scramMechanism, salt, iterations);

        try {
            ScramFormatter formatter = new ScramFormatter(scramMechanism);

            return new UserScramCredentialRecord()
                .setName(name)
                .setMechanism(scramMechanism.type())
                .setSalt(salt)
                .setStoredKey(formatter.storedKey(formatter.clientKey(saltedPassword)))
                .setServerKey(formatter.serverKey(saltedPassword))
                .setIterations(iterations);
        } catch (Throwable e) {
            throw new TerseException("Error attempting to create UserScramCredentialRecord: " + e.getMessage());
        }
    }

    private static String getName(Map<String, String> argMap) throws TerseException {
        if (!argMap.containsKey("name")) {
            throw new TerseException("You must supply 'name' to add-scram");
        }
        return argMap.get("name");
    }

    private static byte[] getSalt(Map<String, String> argMap, ScramMechanism scramMechanism) throws NoSuchAlgorithmException {
        if (argMap.containsKey("salt")) {
            return Base64.getDecoder().decode(argMap.get("salt"));
        } else {
            return new ScramFormatter(scramMechanism).secureRandomBytes();
        }
    }

    private static int getIterations(Map<String, String> argMap, ScramMechanism scramMechanism) throws TerseException {
        if (argMap.containsKey("iterations")) {
            int iterations = Integer.parseInt(argMap.get("iterations"));
            if (iterations < scramMechanism.minIterations()) {
                throw new TerseException("The 'iterations' value must be >= " + scramMechanism.minIterations() + " for add-scram");
            }
            if (iterations > scramMechanism.maxIterations()) {
                throw new TerseException("The 'iterations' value must be <= " + scramMechanism.maxIterations() + " for add-scram");
            }
            return iterations;
        } else {
            return 4096;
        }
    }

    private static byte[] getSaltedPassword(
        Map<String, String> argMap,
        ScramMechanism scramMechanism,
        byte[] salt,
        int iterations
    ) throws TerseException, NoSuchAlgorithmException, InvalidKeyException {
        if (argMap.containsKey("password")) {
            if (argMap.containsKey("saltedpassword")) {
                throw new TerseException("You must only supply one of 'password' or 'saltedpassword' to add-scram");
            }
            return new ScramFormatter(scramMechanism).saltedPassword(argMap.get("password"), salt, iterations);
        } else {
            if (!argMap.containsKey("saltedpassword")) {
                throw new TerseException("You must supply one of 'password' or 'saltedpassword' to add-scram");
            }
            if (!argMap.containsKey("salt")) {
                throw new TerseException("You must supply 'salt' with 'saltedpassword' to add-scram");
            }
            return Base64.getDecoder().decode(argMap.get("saltedpassword"));
        }
    }
    
    public static Optional<List<UserScramCredentialRecord>> getUserScramCredentialRecords(Namespace namespace) 
            throws TerseException, NoSuchAlgorithmException, InvalidKeyException {
        if (namespace.getList("add_scram") != null) {
            List<String> listOfAddConfig = namespace.getList("add_scram");
            List<UserScramCredentialRecord> userScramCredentialRecords = new ArrayList<>();
            for (String singleAddConfig : listOfAddConfig) {
                String[] singleAddConfigList = singleAddConfig.split("\\s+");

                // The first subarg must be of the form key=value
                String[] nameValueRecord = singleAddConfigList[0].split("=", 2);
                switch (nameValueRecord[0]) {
                    case "SCRAM-SHA-256":
                    case "SCRAM-SHA-512":
                        userScramCredentialRecords.add(getUserScramCredentialRecord(nameValueRecord[0], nameValueRecord[1]));
                        break;
                    default:
                        throw new TerseException("The add-scram mechanism " + nameValueRecord[0] + " is not supported.");
                }
            }
            return Optional.of(userScramCredentialRecords);
        } else {
            return Optional.empty();
        }
    }

    static int infoCommand(PrintStream stream, boolean selfManagedMode, List<String> directories) throws IOException {
        List<String> problems = new ArrayList<>();
        List<String> foundDirectories = new ArrayList<>();
        Optional<MetaProperties> prevMetadata = Optional.empty();
        for (String directory : directories) {
            Path directoryPath = Paths.get(directory);
            if (!Files.isDirectory(directoryPath)) {
                if (!Files.exists(directoryPath)) {
                    problems.add(directoryPath + " does not exist");
                } else {
                    problems.add(directoryPath + " is not a directory");
                }
            } else {
                foundDirectories.add(directoryPath.toString());
                Path metaPath = directoryPath.resolve(MetaPropertiesEnsemble.META_PROPERTIES_NAME);
                if (!Files.exists(metaPath)) {
                    problems.add(directoryPath + " is not formatted.");
                } else {
                    Properties properties = Utils.loadProps(metaPath.toString());
                    try {
                        Optional<MetaProperties> curMetadata = Optional.of(new MetaProperties.Builder(properties).build());
                        if (!prevMetadata.isPresent()) {
                            prevMetadata = curMetadata;
                        } else {
                            if (!prevMetadata.get().clusterId().equals(curMetadata.get().clusterId())) {
                                problems.add("Mismatched cluster IDs between storage directories.");
                            } else if (!prevMetadata.get().nodeId().equals(curMetadata.get().nodeId())) {
                                problems.add("Mismatched node IDs between storage directories.");
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        problems.add("Error loading " + metaPath + ": " + e.getMessage());
                    }
                }
            }
        }

        if (prevMetadata.isPresent()) {
            if (selfManagedMode) {
                if (prevMetadata.get().version().equals(MetaPropertiesVersion.V0)) {
                    problems.add("The kafka configuration file appears to be for a cluster in KRaft mode, " +
                        "but the directories are formatted for legacy mode.");
                }
            } else if (prevMetadata.get().version().equals(MetaPropertiesVersion.V1)) {
                problems.add("The kafka configuration file appears to be for a legacy cluster, " +
                    "but the directories are formatted for a cluster in KRaft mode.");
            }
        }

        return validateDirectories(stream, directories, problems, foundDirectories, prevMetadata);
    }

    private static int validateDirectories(PrintStream stream,
                                           List<String> directories,
                                           List<String> problems,
                                           List<String> foundDirectories,
                                           Optional<MetaProperties> prevMetadata) {
        if (directories.isEmpty()) {
            stream.println("No directories specified.");
            return 0;
        } else {
            if (!foundDirectories.isEmpty()) {
                if (foundDirectories.size() == 1) {
                    stream.println("Found log directory:");
                } else {
                    stream.println("Found log directories:");
                }
                foundDirectories.forEach(d -> stream.println("  " + d));
                stream.println();
            }

            if (prevMetadata.isPresent()) {
                Map<String, String> sortedOutput = new TreeMap<>();
                MetaProperties prev = prevMetadata.get();
                prev.toProperties().entrySet().forEach(e -> sortedOutput.put(e.getKey().toString(), e.getValue().toString()));
                stream.println("Found metadata: " + sortedOutput);
                stream.println("");
            }

            if (!problems.isEmpty()) {
                if (problems.size() == 1) {
                    stream.println("Found problem:");
                } else {
                    stream.println("Found problems:");
                }
                problems.forEach(d -> stream.println("  " + d));
                stream.println("");
                return 1;
            } else {
                return 0;
            }
        }
    }

    public static BootstrapMetadata buildBootstrapMetadata(MetadataVersion metadataVersion,
                                                           Optional<List<ApiMessageAndVersion>> metadataOptionalArguments,
                                                           String source) {
        List<ApiMessageAndVersion> metadataRecords = new ArrayList<>();
        metadataRecords.add(new ApiMessageAndVersion(new FeatureLevelRecord()
            .setName(MetadataVersion.FEATURE_NAME)
            .setFeatureLevel(metadataVersion.featureLevel()), (short) 0));
        
        if (metadataOptionalArguments != null) {
            metadataOptionalArguments.ifPresent(metadataArguments -> metadataArguments.forEach(metadataRecords::add));
        }
        
        return BootstrapMetadata.fromRecords(metadataRecords, source);
    }

    public static int formatCommand(PrintStream stream,
                             List<String> directories,
                             MetaProperties metaProperties,
                             BootstrapMetadata bootstrapMetadata,
                             MetadataVersion metadataVersion,
                             boolean ignoreFormatted) throws TerseException, IOException {
        if (directories.isEmpty()) {
            throw new TerseException("No log directories found in the configuration.");
        }

        MetaPropertiesEnsemble.Loader loader = new MetaPropertiesEnsemble.Loader();
        directories.forEach(loader::addLogDir);
        MetaPropertiesEnsemble metaPropertiesEnsemble = loader.load();
        metaPropertiesEnsemble.verify(metaProperties.clusterId(), metaProperties.nodeId(), EnumSet.noneOf(MetaPropertiesEnsemble.VerificationFlag.class));

        System.out.println("metaPropertiesEnsemble=" + metaPropertiesEnsemble);
        MetaPropertiesEnsemble.Copier copier = new MetaPropertiesEnsemble.Copier(metaPropertiesEnsemble);
        if (!(ignoreFormatted || copier.logDirProps().isEmpty())) {
            String firstLogDir = copier.logDirProps().keySet().iterator().next();
            throw new TerseException("Log directory " + firstLogDir + " is already formatted. " +
                "Use --ignore-formatted to ignore this directory and format the others.");
        }
        if (!copier.errorLogDirs().isEmpty()) {
            copier.errorLogDirs().forEach(errorLogDir -> 
                stream.printf("I/O error trying to read log directory %s. Ignoring...%n", errorLogDir));
            if (metaPropertiesEnsemble.emptyLogDirs().isEmpty() && copier.logDirProps().isEmpty()) {
                throw new TerseException("No available log directories to format.");
            }
        }
        if (metaPropertiesEnsemble.emptyLogDirs().isEmpty()) {
            stream.println("All of the log directories are already formatted.");
        } else {
            metaPropertiesEnsemble.emptyLogDirs().forEach(logDir -> {
                copier.setLogDirProps(logDir, new MetaProperties.Builder(metaProperties).
                    setDirectoryId(copier.generateValidDirectoryId()).
                    build());
                copier.setPreWriteHandler((logDir1, isNew, metaProperties1) -> {
                    stream.println("Formatting " + logDir1 + " with metadata.version " + metadataVersion + ".");
                    Files.createDirectories(Paths.get(logDir1));
                    BootstrapDirectory bootstrapDirectory = new BootstrapDirectory(logDir1, Optional.empty());
                    bootstrapDirectory.writeBinaryFile(bootstrapMetadata);
                });
                copier.setWriteErrorHandler((logDir2, e) -> {
                    throw new RuntimeException("Error while writing meta.properties file " + logDir2 + ": " + e.getMessage());
                });
                try {
                    copier.writeLogDirChanges();
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return 0;
    }

    public static List<String> logDirs(LogConfig logConfig) {
        String logDirsConfig = (String) logConfig.originals().get(ServerLogConfigs.LOG_DIRS_CONFIG);
        String logDirConfig = (String) logConfig.originals().get(ServerLogConfigs.LOG_DIR_CONFIG);
        String csvList = logDirsConfig != null ? logDirsConfig : logDirConfig;
        if (csvList == null || csvList.isEmpty()) {
            return Collections.emptyList();
        } else {
            return Arrays.stream(csvList.split("\\s*,\\s*"))
                .filter(v -> !v.isEmpty())
                .collect(Collectors.toList());
        }
    }

    public static Optional<String> interBrokerProtocolVersion(LogConfig logConfig) {
        String originalIBP = (String) logConfig.originals().get(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG);
        return originalIBP != null ? Optional.of(originalIBP) : Optional.empty();
    }

    public static Boolean unstableMetadataVersionsEnabled(LogConfig logConfig) {
        return Boolean.valueOf((String) logConfig.originals().get(ServerConfigs.UNSTABLE_METADATA_VERSIONS_ENABLE_CONFIG));
    }

    public static String metadataLogDir(LogConfig logConfig) {
        String metadataLogDir = (String) logConfig.originals().get(KRaftConfigs.METADATA_LOG_DIR_CONFIG);
        if (metadataLogDir != null) {
            return metadataLogDir;
        } else {
            return logDirs(logConfig).get(0);
        }
    }

    public static Integer nodeId(LogConfig logConfig) { 
        return Integer.valueOf((String) logConfig.originals().get(KRaftConfigs.NODE_ID_CONFIG));
    }

    public static Set<ProcessRole> parseProcessRoles(List<String> processRoles) {
        Set<ProcessRole> distinctRoles = new HashSet<>();
        for (String role : processRoles) {
            switch (role) {
                case "broker":
                    distinctRoles.add(ProcessRole.BrokerRole);
                    break;
                case "controller":
                    distinctRoles.add(ProcessRole.ControllerRole);
                    break;
                default:
                    throw new ConfigException("Unknown process role '" + role + "'" +
                        " (only 'broker' and 'controller' are allowed roles)");
            }
        }
        if (distinctRoles.size() != processRoles.size()) {
            throw new ConfigException("Duplicate role names found in '" + KRaftConfigs.PROCESS_ROLES_CONFIG + "': " + processRoles);
        }
        return distinctRoles;
    }
}
