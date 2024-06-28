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

import org.apache.kafka.metadata.bootstrap.BootstrapDirectory;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.properties.MetaProperties;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.metadata.properties.MetaPropertiesVersion;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.FeatureVersion;
import org.apache.kafka.server.common.Features;
import org.apache.kafka.server.common.MetadataVersion;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

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
     */
    private Map<String, Short> featureLevels = new TreeMap<>();

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
        releaseVersion = calculateEffectiveReleaseVersion();
        featureLevels = calculateEffectiveFeatureLevels();
        this.bootstrapMetadata = calculateBootstrapMetadata();
        doFormat(bootstrapMetadata);
    }

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
                            ". Supported features are: " + nameToSupportedFeature.keySet().stream().
                            collect(Collectors.joining(", ")));
                }
            }
            newFeatureLevels.put(featureName, level);
        }
        newFeatureLevels.put(MetadataVersion.FEATURE_NAME, releaseVersion.featureLevel());
        // Add default values for features that were not specified.
        supportedFeatures.forEach(supportedFeature -> {
            if (!newFeatureLevels.containsKey(supportedFeature.featureName())) {
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
            ensemble.emptyLogDirs().forEach(logDir -> {
                copier.setLogDirProps(logDir, new MetaProperties.Builder(metaProperties).
                    setDirectoryId(copier.generateValidDirectoryId()).
                    build());
                copier.setPreWriteHandler((writeLogDir, __, ____) -> {
                    printStream.println("Formatting " + writeLogDir + " with metadata.version " + releaseVersion + ".");
                    Files.createDirectories(Paths.get(writeLogDir));
                    BootstrapDirectory bootstrapDirectory = new BootstrapDirectory(writeLogDir, Optional.empty());
                    bootstrapDirectory.writeBinaryFile(bootstrapMetadata);
                });
                copier.setWriteErrorHandler((errorLogDir, e) -> {
                    throw new FormatterException("Error while writing meta.properties file " +
                            errorLogDir + ": " + e);
                });
            });
            copier.writeLogDirChanges();
        }
    }
}
