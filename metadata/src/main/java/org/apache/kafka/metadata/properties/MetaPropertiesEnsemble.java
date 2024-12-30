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

package org.apache.kafka.metadata.properties;

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.Uuid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * A collection of meta.properties information for Kafka log directories.
 *
 * Directories are categorized into empty, error, and normal. Each directory must appear in only
 * one category, corresponding to emptyLogDirs, errorLogDirs, and logDirProps.
 *
 * This class is immutable. Modified copies can be made with the Copier class.
 */
public final class MetaPropertiesEnsemble {
    /**
     * The log4j object for this class.
     */
    public static final Logger LOG = LoggerFactory.getLogger(MetaPropertiesEnsemble.class);

    /**
     * A completely empty MetaPropertiesEnsemble object.
     */
    public static final MetaPropertiesEnsemble EMPTY = new MetaPropertiesEnsemble(Collections.emptySet(),
        Collections.emptySet(),
        Collections.emptyMap(),
        Optional.empty());

    /**
     * The name of the meta.properties file within each log directory.
     */
    public static final String META_PROPERTIES_NAME = "meta.properties";

    /**
     * The set of log dirs that were empty.
     */
    private final Set<String> emptyLogDirs;

    /**
     * The set of log dirs that had errors.
     */
    private final Set<String> errorLogDirs;

    /**
     * A map from log directories to the meta.properties information inside each one.
     */
    private final Map<String, MetaProperties> logDirProps;

    /**
     * The metadata log directory, or the empty string if there is none.
     */
    private final Optional<String> metadataLogDir;

    /**
     * Utility class for loading a MetaPropertiesEnsemble from the disk.
     */
    public static class Loader {
        private final TreeSet<String> logDirs = new TreeSet<>();
        private Optional<String> metadataLogDir = Optional.empty();

        public Loader addLogDirs(Collection<String> logDirs) {
            this.logDirs.addAll(logDirs);
            return this;
        }

        public Loader addMetadataLogDir(String metadataLogDir) {
            if (this.metadataLogDir.isPresent()) {
                throw new RuntimeException("Cannot specify more than one metadata log directory. " +
                    "Already specified " + this.metadataLogDir.get());
            }
            this.metadataLogDir = Optional.of(metadataLogDir);
            logDirs.add(metadataLogDir);
            return this;
        }

        public MetaPropertiesEnsemble load() throws IOException  {
            if (logDirs.isEmpty()) {
                throw new RuntimeException("You must specify at least one log directory.");
            }
            Set<String> emptyLogDirs = new HashSet<>();
            Set<String> errorLogDirs = new HashSet<>();
            Map<String, MetaProperties> logDirProps = new HashMap<>();
            for (String logDir : logDirs) {
                String metaPropsFile = new File(logDir, META_PROPERTIES_NAME).getAbsolutePath();
                try {
                    Properties props = PropertiesUtils.readPropertiesFile(metaPropsFile);
                    MetaProperties meta = new MetaProperties.Builder(props).build();
                    logDirProps.put(logDir, meta);
                } catch (NoSuchFileException | FileNotFoundException e) {
                    emptyLogDirs.add(logDir);
                } catch (Exception e) {
                    LOG.error("Error while reading meta.properties file {}", metaPropsFile, e);
                    errorLogDirs.add(logDir);
                }
            }
            return new MetaPropertiesEnsemble(emptyLogDirs, errorLogDirs, logDirProps, metadataLogDir);
        }
    }

    public interface WriteErrorHandler {
        void handle(
            String logDir,
            IOException e
        ) throws IOException;
    }

    public interface PreWriteHandler {
        void handle(
            String logDir,
            boolean isNew,
            MetaProperties metaProperties
        ) throws IOException;
    }

    /**
     * Utility class for copying a MetaPropertiesEnsemble object, possibly with changes.
     */
    public static class Copier {

        private final MetaPropertiesEnsemble prev;
        private final Set<String> emptyLogDirs;
        private final Set<String> errorLogDirs;
        private final Map<String, MetaProperties> logDirProps;
        private Optional<String> metaLogDir;
        private Random random = new Random();

        private PreWriteHandler preWriteHandler = (logDir, isNew, metaProperties) -> {
            LOG.info("Writing out {} {}{}meta.properties file containing {}",
                    isNew ? "new" : "changed",
                    logDir,
                    File.separator,
                    metaProperties);
        };

        private WriteErrorHandler writeErrorHandler = (logDir, e) -> {
            LOG.error("Error while writing meta.properties to {}", logDir, e);
            throw e;
        };

        public Copier(MetaPropertiesEnsemble prev) {
            this.prev = prev;
            this.emptyLogDirs = new HashSet<>(prev.emptyLogDirs());
            this.errorLogDirs = new HashSet<>(prev.errorLogDirs());
            this.logDirProps = new HashMap<>(prev.logDirProps());
            this.metaLogDir = prev.metadataLogDir;
        }

        /**
         * Set the Random object to use for generating IDs.
         *
         * @param random    The Random object to use for generating IDs.
         * @return          This copier
         */
        public Copier setRandom(Random random) {
            this.random = random;
            return this;
        }

        /**
         * Access the mutable empty log directories set.
         *
         * @return          The mutable empty log directories set.
         */
        public Set<String> emptyLogDirs() {
            return emptyLogDirs;
        }

        /**
         * Access the mutable error log directories set.
         *
         * @return          The mutable error log directories set.
         */
        public Set<String> errorLogDirs() {
            return errorLogDirs;
        }

        /**
         * Access the mutable logDirProps map.
         *
         * @return          The mutable error log directories map.
         */
        public Map<String, MetaProperties> logDirProps() {
            return logDirProps;
        }

        /**
         * Set the properties of a given log directory.
         *
         * @param logDir    The log directory path.
         * @param metaProps The properties to set.
         *
         * @return          This copier.
         */
        public Copier setLogDirProps(String logDir, MetaProperties metaProps) {
            emptyLogDirs.remove(logDir);
            errorLogDirs.remove(logDir);
            logDirProps.put(logDir, metaProps);
            return this;
        }

        public Optional<String> metaLogDir() {
            return metaLogDir;
        }

        /**
         * Set the current metadata log directory.
         *
         * @param metaLogDir    The metadata log directory, or Optional.empty if there is none.
         *
         * @return              This copier.
         */
        public Copier setMetaLogDir(Optional<String> metaLogDir) {
            this.metaLogDir = metaLogDir;
            return this;
        }

        /**
         * Generate a random directory ID that is safe and not used by any other directory.
         *
         * @return          A new random directory ID.
         */
        public Uuid generateValidDirectoryId() {
            while (true) {
                Uuid uuid = new Uuid(random.nextLong(), random.nextLong());
                if (!(uuid.toString().startsWith("-") || DirectoryId.reserved(uuid))) {
                    boolean duplicate = false;
                    for (MetaProperties metaProps : logDirProps.values()) {
                        if (metaProps.directoryId().equals(Optional.of(uuid))) {
                            duplicate = true;
                            break;
                        }
                    }
                    if (!duplicate) {
                        return uuid;
                    }
                }
            }
        }

        /**
         * Set the pre-write handler.
         *
         * @param preWriteHandler  A handler that will be called before we try to write to a
         *                         directory.
         */
        public Copier setPreWriteHandler(PreWriteHandler preWriteHandler) {
            this.preWriteHandler = preWriteHandler;
            return this;
        }

        /**
         * Set the error handler.
         *
         * @param writeErrorHandler A handler that will be called any time we hit an exception
         *                          writing out a meta.properties file.
         */
        public Copier setWriteErrorHandler(WriteErrorHandler writeErrorHandler) {
            this.writeErrorHandler = writeErrorHandler;
            return this;
        }

        /**
         * Verify that we have set up the Copier correctly.
         *
         * @throws RuntimeException if a directory appears in more than one category.
         */
        public void verify() {
            for (String logDir : emptyLogDirs) {
                if (errorLogDirs.contains(logDir)) {
                    throw new RuntimeException("Error: log directory " + logDir +
                        " is in both emptyLogDirs and errorLogDirs.");
                }
                if (logDirProps.containsKey(logDir)) {
                    throw new RuntimeException("Error: log directory " + logDir +
                        " is in both emptyLogDirs and logDirProps.");
                }
            }
            for (String logDir : errorLogDirs) {
                if (logDirProps.containsKey(logDir)) {
                    throw new RuntimeException("Error: log directory " + logDir +
                            " is in both errorLogDirs and logDirProps.");
                }
            }
            metaLogDir().ifPresent(m -> {
                if (!(emptyLogDirs.contains(m) ||
                        logDirProps.containsKey(m) ||
                        errorLogDirs.contains(m))) {
                    throw new RuntimeException("Error: metaLogDir " + m + " does not appear " +
                        "in emptyLogDirs, errorLogDirs, or logDirProps.");
                }
            });
        }

        /**
         * Write any changed log directories out to disk.
         */
        public void writeLogDirChanges() throws IOException {
            Map<String, MetaProperties> newOrChanged = new HashMap<>();
            HashSet<String> newSet = new HashSet<>();
            for (Entry<String, MetaProperties> entry : prev.logDirProps().entrySet()) {
                MetaProperties prevMetaProps = entry.getValue();
                MetaProperties metaProps = logDirProps.get(entry.getKey());
                if (!prevMetaProps.equals(metaProps)) {
                    newOrChanged.put(entry.getKey(), metaProps);
                }
            }
            for (Entry<String, MetaProperties> entry : logDirProps.entrySet()) {
                if (!prev.logDirProps().containsKey(entry.getKey())) {
                    newOrChanged.put(entry.getKey(), entry.getValue());
                    newSet.add(entry.getKey());
                }
            }
            for (Entry<String, MetaProperties> entry : newOrChanged.entrySet()) {
                String logDir = entry.getKey();
                MetaProperties metaProps = entry.getValue();
                String metaPropsPath = new File(logDir, META_PROPERTIES_NAME).getAbsolutePath();
                try {
                    preWriteHandler.handle(logDir, newSet.contains(logDir), metaProps);
                    PropertiesUtils.writePropertiesFile(metaProps.toProperties(),
                        metaPropsPath, true);
                } catch (IOException e) {
                    errorLogDirs.add(logDir);
                    logDirProps.remove(logDir);
                    writeErrorHandler.handle(logDir, e);
                }
            }
        }

        /**
         * Create a new immutable MetaPropertiesEnsemble file.
         *
         * @return  A new MetaPropertiesEnsemble file containing the changes we made in this Copier.
         */
        public MetaPropertiesEnsemble copy() {
            return new MetaPropertiesEnsemble(emptyLogDirs,
                errorLogDirs,
                logDirProps,
                metaLogDir);
        }
    }

    MetaPropertiesEnsemble(
        Set<String> emptyLogDirs,
        Set<String> errorLogDirs,
        Map<String, MetaProperties> logDirProps,
        Optional<String> metadataLogDir
    ) {
        this.emptyLogDirs = Collections.unmodifiableSet(new TreeSet<>(emptyLogDirs));
        this.errorLogDirs = Collections.unmodifiableSet(new TreeSet<>(errorLogDirs));
        this.logDirProps = Collections.unmodifiableMap(new TreeMap<>(logDirProps));
        this.metadataLogDir = metadataLogDir;
    }

    /**
     * @return The empty log directories which did not contain a meta.properties file.
     */
    public Set<String> emptyLogDirs() {
        return emptyLogDirs;
    }

    /**
     * @return The log directories that had I/O errors.
     */
    public Set<String> errorLogDirs() {
        return errorLogDirs;
    }

    /**
     * @return A map from log directory paths to properties objects.
     */
    public Map<String, MetaProperties> logDirProps() {
        return logDirProps;
    }

    /**
     * @return An iterator that returns (logDir, metaProps) for all non-failed directories.
     */
    public Iterator<Entry<String, Optional<MetaProperties>>> nonFailedDirectoryProps() {
        return new Iterator<>() {
            private final Iterator<String> emptyLogDirsIterator = emptyLogDirs.iterator();
            private final Iterator<Entry<String, MetaProperties>> logDirsIterator =
                    logDirProps.entrySet().iterator();

            @Override
            public boolean hasNext() {
                return emptyLogDirsIterator.hasNext() || logDirsIterator.hasNext();
            }

            @Override
            public Entry<String, Optional<MetaProperties>> next() {
                if (emptyLogDirsIterator.hasNext()) {
                    return new SimpleImmutableEntry<>(emptyLogDirsIterator.next(), Optional.empty());
                }
                Entry<String, MetaProperties> entry = logDirsIterator.next();
                return new SimpleImmutableEntry<>(entry.getKey(), Optional.of(entry.getValue()));
            }
        };
    }

    /**
     * @return The metadata log directory, or Optional.empty if there is none.
     */
    public Optional<String> metadataLogDir() {
        return metadataLogDir;
    }

    public enum VerificationFlag {
        REQUIRE_V0,
        REQUIRE_METADATA_LOG_DIR,
        REQUIRE_AT_LEAST_ONE_VALID
    }

    /**
     * Verify that the metadata properties ensemble is valid.
     *
     * We verify that v1 meta.properties files always have cluster.id set. v0 files may or may not
     * have it set. If it is set, the cluster ID must be the same in all directories.
     *
     * We verify that v1 meta.properties files always have node.id set. v0 files may or may not have
     * it set. If it is set in v0, it will be called broker.id rather than node.id. Node ID must be
     * the same in call directories.
     *
     * directory.id may or may not be set, in both v0 and v1. If it is set, it must not be the same
     * in multiple directories, and it must be safe.
     *
     * @param expectedClusterId     The cluster ID to expect, or the empty string if we don't know yet.
     * @param expectedNodeId        The node ID to expect, or -1 if we don't know yet.
     * @param verificationFlags     The flags to use.
     */
    public void verify(
        Optional<String> expectedClusterId,
        OptionalInt expectedNodeId,
        EnumSet<VerificationFlag> verificationFlags
    ) {
        Map<Uuid, String> seenUuids = new HashMap<>();
        if (verificationFlags.contains(VerificationFlag.REQUIRE_AT_LEAST_ONE_VALID)) {
            if (logDirProps.isEmpty()) {
                throw new RuntimeException("No readable meta.properties files found.");
            }
        }
        for (Entry<String, MetaProperties> entry : logDirProps.entrySet()) {
            String logDir = entry.getKey();
            String path = new File(logDir, META_PROPERTIES_NAME).toString();
            MetaProperties metaProps = entry.getValue();
            if (verificationFlags.contains(VerificationFlag.REQUIRE_V0)) {
                if (!metaProps.version().equals(MetaPropertiesVersion.V0)) {
                    throw new RuntimeException("Found unexpected version in " + path + ". " +
                        "ZK-based brokers that are not migrating only support version 0 " +
                        "(which is implicit when the `version` field is missing).");
                }
            }
            if (metaProps.clusterId().isEmpty()) {
                if (metaProps.version().alwaysHasClusterId()) {
                    throw new RuntimeException("cluster.id was not specified in the v1 file: " +
                        path);
                }
            } else if (expectedClusterId.isEmpty()) {
                expectedClusterId = metaProps.clusterId();
            } else if (!metaProps.clusterId().get().equals(expectedClusterId.get())) {
                throw new RuntimeException("Invalid cluster.id in: " + path + ". Expected " +
                    expectedClusterId.get() + ", but read " + metaProps.clusterId().get());
            }
            if (metaProps.nodeId().isEmpty()) {
                if (metaProps.version().alwaysHasNodeId()) {
                    throw new RuntimeException("node.id was not specified in " + path);
                }
            } else if (expectedNodeId.isEmpty()) {
                expectedNodeId = metaProps.nodeId();
            } else if (metaProps.nodeId().getAsInt() != expectedNodeId.getAsInt()) {
                throw new RuntimeException("Stored node id " + metaProps.nodeId().getAsInt() +
                    " doesn't match previous node id " + expectedNodeId.getAsInt() + " in " + path +
                    ". If you moved your data, make sure your configured node id matches. If you " +
                    "intend to create a new node, you should remove all data in your data " +
                    "directories.");
            }
            if (metaProps.directoryId().isPresent()) {
                if (DirectoryId.reserved(metaProps.directoryId().get())) {
                    throw new RuntimeException("Invalid resrved directory ID " +
                        metaProps.directoryId().get() + " found in " + logDir);
                }
                String prevLogDir = seenUuids.put(metaProps.directoryId().get(), logDir);
                if (prevLogDir != null) {
                    throw new RuntimeException("Duplicate directory ID " + metaProps.directoryId() +
                        " found. It was the ID of " + prevLogDir + ", " + "but also of " +
                        logDir);
                }
            }
        }
        if (verificationFlags.contains(VerificationFlag.REQUIRE_METADATA_LOG_DIR)) {
            if (metadataLogDir.isEmpty()) {
                throw new RuntimeException("No metadata log directory was specified.");
            }
        }
        if (metadataLogDir.isPresent()) {
            if (errorLogDirs.contains(metadataLogDir.get())) {
                throw new RuntimeException("Encountered I/O error in metadata log directory " +
                        metadataLogDir.get() + ". Cannot continue.");
            }
        }
    }

    /**
     * Find the node ID of this meta.properties ensemble.
     *
     * @return  The node ID, or OptionalInt.empty if none could be found.
     */
    public OptionalInt nodeId() {
        for (MetaProperties metaProps : logDirProps.values()) {
            if (metaProps.nodeId().isPresent()) {
                return metaProps.nodeId();
            }
        }
        return OptionalInt.empty();
    }

    /**
     * Find the cluster ID of this meta.properties ensemble.
     *
     * @return  The cluster ID, or Optional.empty if none could be found.
     */
    public Optional<String> clusterId() {
        for (MetaProperties metaProps : logDirProps.values()) {
            if (metaProps.clusterId().isPresent()) {
                return metaProps.clusterId();
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (!(o.getClass().equals(MetaPropertiesEnsemble.class)))) {
            return false;
        }
        MetaPropertiesEnsemble other = (MetaPropertiesEnsemble) o;
        return emptyLogDirs.equals(other.emptyLogDirs) &&
            errorLogDirs.equals(other.errorLogDirs) &&
            logDirProps.equals(other.logDirProps) &&
            metadataLogDir.equals(other.metadataLogDir);
    }

    @Override
    public int hashCode() {
        return Objects.hash(emptyLogDirs,
            errorLogDirs,
            logDirProps,
            metadataLogDir);
    }

    @Override
    public String toString() {
        TreeMap<String, String> outputMap = new TreeMap<>();
        emptyLogDirs.forEach(e -> outputMap.put(e, "EMPTY"));
        errorLogDirs.forEach(e -> outputMap.put(e, "ERROR"));
        logDirProps.forEach((key, value) -> outputMap.put(key, value.toString()));
        return "MetaPropertiesEnsemble" +
            "(metadataLogDir=" + metadataLogDir +
            ", dirs={" + outputMap.entrySet().stream().
            map(e -> e.getKey() + ": " + e.getValue()).
            collect(Collectors.joining(", ")) +
            "})";
    }
}
