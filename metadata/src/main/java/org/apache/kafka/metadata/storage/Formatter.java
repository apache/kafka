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

package org.apache.kafka.metadata.storage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.metadata.bootstrap.BootstrapDirectory;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.properties.MetaProperties;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.metadata.properties.MetaPropertiesVersion;
import org.apache.kafka.raft.DynamicVoters;
import org.apache.kafka.raft.KafkaRaftClient;
import org.apache.kafka.raft.VoterSet;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.FeatureVersion;
import org.apache.kafka.server.common.Features;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.snapshot.FileRawSnapshotWriter;
import org.apache.kafka.snapshot.RecordsSnapshotWriter;
import org.apache.kafka.snapshot.Snapshots;

import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.kafka.common.internals.Topic.CLUSTER_METADATA_TOPIC_PARTITION;
import static org.apache.kafka.server.common.KRaftVersion.KRAFT_VERSION_0;
import static org.apache.kafka.server.common.KRaftVersion.KRAFT_VERSION_1;

/**
 * Formats storage directories.
 */
public class Formatter {
    /**
     * The stream to log to while formatting.
     */
    private PrintStream printStream = System.out;

    /**
     * The features that are supported.
     */
    private List<Features> supportedFeatures = Features.PRODUCTION_FEATURES;

    /**
     * The current node id.
     */
    private int nodeId = -1;

    /**
     * The cluster ID to use.
     */
    private String clusterId = null;

    /**
     * The directories to format.
     */
    private final TreeSet<String> directories = new TreeSet<>();

    /**
     * The metadata version to use.
     */
    private MetadataVersion releaseVersion = null;

    /**
     * Maps feature names to the level they will start off with.
     *
     * Visible for testing.
     */
    protected Map<String, Short> featureLevels = new TreeMap<>();

    /**
     * The bootstrap metadata used to format the cluster.
     */
    private BootstrapMetadata bootstrapMetadata;

    /**
     * True if we should enable unstable feature versions.
     */
    private boolean unstableFeatureVersionsEnabled = false;

    /**
     * True if we should ignore already formatted directories.
     */
    private boolean ignoreFormatted = false;

    /**
     * The arguments passed to --add-scram
     */
    private List<String> scramArguments = Collections.emptyList();

    /**
     * The name of the initial controller listener.
     */
    private String controllerListenerName = null;

    /**
     * The metadata log directory.
     */
    private Optional<String> metadataLogDirectory = Optional.empty();

    /**
     * The initial KIP-853 voters.
     */
    private Optional<DynamicVoters> initialControllers = Optional.empty();
    private boolean noInitialControllersFlag = false;

    public Formatter setPrintStream(PrintStream printStream) {
        this.printStream = printStream;
        return this;
    }

    public Formatter setSupportedFeatures(List<Features> supportedFeatures) {
        this.supportedFeatures = supportedFeatures;
        return this;
    }

    public Formatter setNodeId(int nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public Formatter setClusterId(String clusterId) {
        this.clusterId = clusterId;
        return this;
    }

    public String clusterId() {
        return clusterId;
    }

    public Formatter setDirectories(Collection<String> directories) {
        this.directories.clear();
        this.directories.addAll(directories);
        return this;
    }

    public Formatter addDirectory(String directory) {
        this.directories.add(directory);
        return this;
    }

    public Collection<String> directories() {
        return directories;
    }

    public Formatter setReleaseVersion(MetadataVersion releaseVersion) {
        this.releaseVersion = releaseVersion;
        return this;
    }

    public Formatter setFeatureLevel(String featureName, Short level) {
        this.featureLevels.put(featureName, level);
        return this;
    }

    public Formatter setUnstableFeatureVersionsEnabled(boolean unstableFeatureVersionsEnabled) {
        this.unstableFeatureVersionsEnabled = unstableFeatureVersionsEnabled;
        return this;
    }

    public Formatter setIgnoreFormatted(boolean ignoreFormatted) {
        this.ignoreFormatted = ignoreFormatted;
        return this;
    }

    public Formatter setScramArguments(List<String> scramArguments) {
        this.scramArguments = scramArguments;
        return this;
    }

    public Formatter setControllerListenerName(String controllerListenerName) {
        this.controllerListenerName = controllerListenerName;
        return this;
    }

    public Formatter setMetadataLogDirectory(String metadataLogDirectory) {
        this.metadataLogDirectory = Optional.of(metadataLogDirectory);
        return this;
    }

    public Formatter setMetadataLogDirectory(Optional<String> metadataLogDirectory) {
        this.metadataLogDirectory = metadataLogDirectory;
        return this;
    }

    public Formatter setInitialControllers(DynamicVoters initialControllers) {
        this.initialControllers = Optional.of(initialControllers);
        return this;
    }

    public Formatter setNoInitialControllersFlag(boolean noInitialControllersFlag) {
        this.noInitialControllersFlag = noInitialControllersFlag;
        return this;
    }

    public Optional<DynamicVoters> initialVoters() {
        return initialControllers;
    }

    boolean hasDynamicQuorum() {
        return initialControllers.isPresent() || noInitialControllersFlag;
    }

    public BootstrapMetadata bootstrapMetadata() {
        return bootstrapMetadata;
    }

    public void run() throws Exception {
        if (nodeId < 0) {
            throw new RuntimeException("You must specify a valid non-negative node ID.");
        }
        if (clusterId == null) {
            throw new FormatterException("You must specify the cluster id.");
        }
        if (directories.isEmpty()) {
            throw new FormatterException("You must specify at least one directory to format");
        }
        if (controllerListenerName == null) {
            throw new FormatterException("You must specify the name of the initial controller listener.");
        }
        metadataLogDirectory.ifPresent(d -> {
            if (!directories.contains(d)) {
                throw new FormatterException("The specified metadata log directory, " + d +
                    " was not one of the given directories: " + directories);
            }
        });
        releaseVersion = calculateEffectiveReleaseVersion();
        featureLevels = calculateEffectiveFeatureLevels();
        this.bootstrapMetadata = calculateBootstrapMetadata();
        doFormat(bootstrapMetadata);
    }

    /**
     * Calculate the effective value of release version. This will be used to set defaults
     * for the other features. We also throw an exception if something inconsistent was requested.
     *
     * @return  The effective value of release version.
     */
    MetadataVersion calculateEffectiveReleaseVersion() {
        if (featureLevels.containsKey(MetadataVersion.FEATURE_NAME)) {
            if (releaseVersion != null) {
                throw new FormatterException("Use --release-version instead of " +
                    "--feature " + MetadataVersion.FEATURE_NAME + "=X to avoid ambiguity.");
            }
            return verifyReleaseVersion(MetadataVersion.fromFeatureLevel(
                    featureLevels.get(MetadataVersion.FEATURE_NAME)));
        } else if (releaseVersion != null) {
            return verifyReleaseVersion(releaseVersion);
        } else if (unstableFeatureVersionsEnabled) {
            return MetadataVersion.latestTesting();
        } else {
            return MetadataVersion.latestProduction();
        }
    }

    MetadataVersion verifyReleaseVersion(MetadataVersion metadataVersion) {
        if (!metadataVersion.isKRaftSupported()) {
            throw new FormatterException(MetadataVersion.FEATURE_NAME + " " + metadataVersion +
                " is too old to be supported.");
        }
        if (!unstableFeatureVersionsEnabled) {
            if (!metadataVersion.isProduction()) {
                throw new FormatterException(MetadataVersion.FEATURE_NAME + " " + metadataVersion +
                        " is not yet stable.");
            }
        }
        return metadataVersion;
    }

    Map<String, Short> calculateEffectiveFeatureLevels() {
        Map<String, Features> nameToSupportedFeature = new TreeMap<>();
        supportedFeatures.forEach(feature -> nameToSupportedFeature.put(feature.featureName(), feature));
        Map<String, Short> newFeatureLevels = new TreeMap<>();
        // Verify that all specified features are known to us.
        for (Map.Entry<String, Short> entry : featureLevels.entrySet()) {
            String featureName = entry.getKey();
            short level = entry.getValue();
            if (!featureName.equals(MetadataVersion.FEATURE_NAME)) {
                if (!nameToSupportedFeature.containsKey(featureName)) {
                    throw new FormatterException("Unsupported feature: " + featureName +
                            ". Supported features are: " + String.join(", ", nameToSupportedFeature.keySet()));
                }
            }
            newFeatureLevels.put(featureName, level);
        }
        newFeatureLevels.put(MetadataVersion.FEATURE_NAME, releaseVersion.featureLevel());
        // Add default values for features that were not specified.
        supportedFeatures.forEach(supportedFeature -> {
            if (supportedFeature.featureName().equals(KRaftVersion.FEATURE_NAME)) {
                newFeatureLevels.put(KRaftVersion.FEATURE_NAME, effectiveKRaftFeatureLevel(
                    Optional.ofNullable(newFeatureLevels.get(KRaftVersion.FEATURE_NAME))));
            } else if (!newFeatureLevels.containsKey(supportedFeature.featureName())) {
                newFeatureLevels.put(supportedFeature.featureName(),
                    supportedFeature.defaultValue(releaseVersion));
            }
        });
        // Verify that the specified features support the given levels. This requires the full
        // features map since there may be cross-feature dependencies.
        for (Map.Entry<String, Short> entry : newFeatureLevels.entrySet()) {
            String featureName = entry.getKey();
            if (!featureName.equals(MetadataVersion.FEATURE_NAME)) {
                short level = entry.getValue();
                Features supportedFeature = nameToSupportedFeature.get(featureName);
                FeatureVersion featureVersion =
                    supportedFeature.fromFeatureLevel(level, unstableFeatureVersionsEnabled);
                Features.validateVersion(featureVersion, newFeatureLevels);
            }
        }
        return newFeatureLevels;
    }

    /**
     * Calculate the effective feature level for kraft.version. In order to keep existing
     * command-line invocations of StorageTool working, we default this to 0 if no dynamic
     * voter quorum arguments were provided. As a convenience, if dynamic voter quorum arguments
     * were passed, we set the latest kraft.version. (Currently there is only 1 non-zero version).
     *
     * @param configuredKRaftVersionLevel   The configured level for kraft.version
     * @return                              The effective feature level.
     */
    private short effectiveKRaftFeatureLevel(Optional<Short> configuredKRaftVersionLevel) {
        if (configuredKRaftVersionLevel.isPresent()) {
            if (configuredKRaftVersionLevel.get() == 0) {
                if (hasDynamicQuorum()) {
                    throw new FormatterException("Cannot set kraft.version to " +
                        configuredKRaftVersionLevel.get() + " if KIP-853 configuration is present. " +
                            "Try removing the --feature flag for kraft.version.");
                }
            } else {
                if (!hasDynamicQuorum()) {
                    throw new FormatterException("Cannot set kraft.version to " +
                        configuredKRaftVersionLevel.get() + " unless KIP-853 configuration is present. " +
                            "Try removing the --feature flag for kraft.version.");
                }
            }
            return configuredKRaftVersionLevel.get();
        } else if (hasDynamicQuorum()) {
            return KRAFT_VERSION_1.featureLevel();
        } else {
            return KRAFT_VERSION_0.featureLevel();
        }
    }

    BootstrapMetadata calculateBootstrapMetadata() throws  Exception {
        BootstrapMetadata bootstrapMetadata = BootstrapMetadata.
            fromVersions(releaseVersion, featureLevels, "format command");
        List<ApiMessageAndVersion> bootstrapRecords = new ArrayList<>(bootstrapMetadata.records());
        if (!scramArguments.isEmpty()) {
            if (!releaseVersion.isScramSupported()) {
                throw new FormatterException("SCRAM is only supported in " + MetadataVersion.FEATURE_NAME +
                        " " + MetadataVersion.IBP_3_5_IV2 + " or later.");
            }
            bootstrapRecords.addAll(ScramParser.parse(scramArguments));
        }
        return BootstrapMetadata.fromRecords(bootstrapRecords, "format command");
    }

    void doFormat(BootstrapMetadata bootstrapMetadata) throws Exception {
        MetaProperties metaProperties = new MetaProperties.Builder().
                setVersion(MetaPropertiesVersion.V1).
                setClusterId(clusterId).
                setNodeId(nodeId).
                build();
        MetaPropertiesEnsemble.Loader loader = new MetaPropertiesEnsemble.Loader();
        loader.addLogDirs(directories);
        MetaPropertiesEnsemble ensemble = loader.load();
        ensemble.verify(Optional.of(clusterId),
                OptionalInt.of(nodeId),
                EnumSet.noneOf(MetaPropertiesEnsemble.VerificationFlag.class));
        MetaPropertiesEnsemble.Copier copier = new MetaPropertiesEnsemble.Copier(ensemble);
        if (!(ignoreFormatted || copier.logDirProps().isEmpty())) {
            String firstLogDir = copier.logDirProps().keySet().iterator().next();
            throw new FormatterException("Log directory " + firstLogDir + " is already formatted. " +
                "Use --ignore-formatted to ignore this directory and format the others.");
        }
        if (!copier.errorLogDirs().isEmpty()) {
            copier.errorLogDirs().forEach(errorLogDir ->
                printStream.println("I/O error trying to read log directory " + errorLogDir + ". Ignoring..."));
            if (ensemble.emptyLogDirs().isEmpty() && copier.logDirProps().isEmpty()) {
                throw new FormatterException("No available log directories to format.");
            }
        }
        if (ensemble.emptyLogDirs().isEmpty()) {
            printStream.println("All of the log directories are already formatted.");
        } else {
            Map<String, DirectoryType> directoryTypes = new HashMap<>();
            for (String emptyLogDir : ensemble.emptyLogDirs()) {
                DirectoryType directoryType = DirectoryType.calculate(emptyLogDir,
                    metadataLogDirectory.orElse(""),
                    nodeId,
                    initialControllers);
                directoryTypes.put(emptyLogDir, directoryType);
                Uuid directoryId;
                if (directoryType == DirectoryType.DYNAMIC_METADATA_VOTER_DIRECTORY) {
                    directoryId = initialControllers.get().voters().get(nodeId).directoryId();
                } else {
                    directoryId = copier.generateValidDirectoryId();
                }
                copier.setLogDirProps(emptyLogDir, new MetaProperties.Builder(metaProperties).
                    setDirectoryId(directoryId).
                    build());
            }
            copier.setPreWriteHandler((writeLogDir, __, ____) -> {
                printStream.printf("Formatting %s %s with %s %s.%n",
                    directoryTypes.get(writeLogDir).description(), writeLogDir,
                    MetadataVersion.FEATURE_NAME, releaseVersion);
                Files.createDirectories(Paths.get(writeLogDir));
                BootstrapDirectory bootstrapDirectory = new BootstrapDirectory(writeLogDir, Optional.empty());
                bootstrapDirectory.writeBinaryFile(bootstrapMetadata);
                if (directoryTypes.get(writeLogDir).isDynamicMetadataDirectory()) {
                    writeDynamicQuorumSnapshot(writeLogDir,
                        initialControllers.get(),
                        featureLevels.get(KRaftVersion.FEATURE_NAME),
                        controllerListenerName);
                }
            });
            copier.setWriteErrorHandler((errorLogDir, e) -> {
                throw new FormatterException("Error while writing meta.properties file " +
                        errorLogDir + ": " + e);
            });
            copier.writeLogDirChanges();
        }
    }

    enum DirectoryType {
        LOG_DIRECTORY,
        STATIC_METADATA_DIRECTORY,
        DYNAMIC_METADATA_NON_VOTER_DIRECTORY,
        DYNAMIC_METADATA_VOTER_DIRECTORY;

        String description() {
            switch (this) {
                case LOG_DIRECTORY:
                    return "data directory";
                case STATIC_METADATA_DIRECTORY:
                    return "metadata directory";
                case DYNAMIC_METADATA_NON_VOTER_DIRECTORY:
                    return "dynamic metadata directory";
                case DYNAMIC_METADATA_VOTER_DIRECTORY:
                    return "dynamic metadata voter directory";
            }
            throw new RuntimeException("invalid enum type " + this);
        }

        boolean isDynamicMetadataDirectory() {
            return this == DYNAMIC_METADATA_NON_VOTER_DIRECTORY ||
                this == DYNAMIC_METADATA_VOTER_DIRECTORY;
        }

        static DirectoryType calculate(
            String logDir,
            String metadataLogDirectory,
            int nodeId,
            Optional<DynamicVoters> initialControllers
        ) {
            if (!logDir.equals(metadataLogDirectory)) {
                return LOG_DIRECTORY;
            } else if (initialControllers.isEmpty()) {
                return STATIC_METADATA_DIRECTORY;
            } else if (initialControllers.get().voters().containsKey(nodeId)) {
                return DYNAMIC_METADATA_VOTER_DIRECTORY;
            } else {
                return DYNAMIC_METADATA_NON_VOTER_DIRECTORY;
            }
        }
    }

    static void writeDynamicQuorumSnapshot(
        String writeLogDir,
        DynamicVoters initialControllers,
        short kraftVersion,
        String controllerListenerName
    ) {
        File parentDir = new File(writeLogDir);
        File clusterMetadataDirectory = new File(parentDir, String.format("%s-%d",
                CLUSTER_METADATA_TOPIC_PARTITION.topic(),
                CLUSTER_METADATA_TOPIC_PARTITION.partition()));
        VoterSet voterSet = initialControllers.toVoterSet(controllerListenerName);
        RecordsSnapshotWriter.Builder builder = new RecordsSnapshotWriter.Builder().
            setLastContainedLogTimestamp(Time.SYSTEM.milliseconds()).
            setMaxBatchSize(KafkaRaftClient.MAX_BATCH_SIZE_BYTES).
            setRawSnapshotWriter(FileRawSnapshotWriter.create(
                clusterMetadataDirectory.toPath(),
                Snapshots.BOOTSTRAP_SNAPSHOT_ID)).
            setKraftVersion(KRaftVersion.fromFeatureLevel(kraftVersion)).
            setVoterSet(Optional.of(voterSet));
        try (RecordsSnapshotWriter writer = builder.build(new MetadataRecordSerde())) {
            writer.freeze();
        }
    }
}
