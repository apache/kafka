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

package org.apache.kafka.metadata.broker;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

public class BrokerMetadataCheckpoint {
    static LogContext logContext = new LogContext("[BrokerMetadataCheckpoint] ");
    static Logger log = logContext.logger(BrokerMetadataCheckpoint.class);
    private final File file;
    private final Object lock = new Object();

    public BrokerMetadataCheckpoint(File file) {
        this.file = file;
    }

    public void write(Properties properties) throws IOException {
        synchronized (lock) {
            try {
                File temp = new File(file.getAbsolutePath() + ".tmp");
                FileOutputStream fileOutputStream = new FileOutputStream(temp);
                try {
                    properties.store(fileOutputStream, "");
                    fileOutputStream.flush();
                    fileOutputStream.getFD().sync();
                } finally {
                    Utils.closeQuietly(fileOutputStream, temp.getName());
                }
                Utils.atomicMoveWithFallback(temp.toPath(), file.toPath());
            } catch (IOException e) {
                log.error("Failed to write meta.properties due to", e);
                throw e;
            }
        }
    }

    public Optional<Properties> read() throws IOException {
        Files.deleteIfExists(new File(file.getPath() + ".tmp").toPath());

        String absolutePath = file.getAbsolutePath();
        synchronized (lock) {
            try {
                return Optional.of(Utils.loadProps(absolutePath));
            } catch (NoSuchFileException e) {
                log.warn("No meta.properties file under dir " + absolutePath);
                return Optional.empty();
            } catch (Exception e) {
                log.error("Failed to read meta.properties file under dir " + absolutePath, e);
                throw e;
            }
        }
    }

    public static Pair<RawMetaProperties, List<String>> getBrokerMetadataAndOfflineDirs(
        List<String> logDirs,
        boolean ignoreMissing,
        boolean kraftMode
    ) {
        if (logDirs.isEmpty()) {
            throw new IllegalArgumentException("Must have at least one log dir to read meta.properties");
        }

        Map<String, Properties> brokerMetadataMap = new HashMap<>();
        List<String> offlineDirs = new ArrayList<>();

        for (String logDir : logDirs) {
            File brokerCheckpointFile = new File(logDir, "meta.properties");
            BrokerMetadataCheckpoint brokerCheckpoint = new BrokerMetadataCheckpoint(brokerCheckpointFile);

            try {
                Optional<Properties> propertiesOptional = brokerCheckpoint.read();
                if (propertiesOptional.isPresent()) {
                    brokerMetadataMap.put(logDir, propertiesOptional.get());
                } else {
                    if (!ignoreMissing) {
                        throw new KafkaException("No `meta.properties` found in " + logDir +
                            " (have you run `kafka-storage.sh` to format the directory?)");
                    }
                }
            } catch (IOException e) {
                offlineDirs.add(logDir);
                log.error("Failed to read " + brokerCheckpointFile, e);
            }
        }

        if (brokerMetadataMap.isEmpty()) {
            return new Pair<>(new RawMetaProperties(new Properties()), offlineDirs);
        } else {
            int numDistinctMetaProperties;
            if (kraftMode) {
                Set<MetaProperties> distinctMetaProperties = new HashSet<>();
                for (Properties props : brokerMetadataMap.values()) {
                    distinctMetaProperties.add(MetaProperties.parse(new RawMetaProperties(props)));
                }

                numDistinctMetaProperties = distinctMetaProperties.size();
            } else {
                numDistinctMetaProperties = brokerMetadataMap.values().size();
            }

            if (numDistinctMetaProperties > 1) {
                StringBuilder builder = new StringBuilder();
                for (Map.Entry<String, Properties> entry : brokerMetadataMap.entrySet()) {
                    builder.append("- ").append(entry.getKey()).append(" -> ").append(entry.getValue()).append("\n");
                }
                throw new InconsistentBrokerMetadataException("BrokerMetadata is not consistent across log.dirs. " +
                    "This could happen if multiple brokers shared a log directory (log.dirs) " +
                    "or partial data was manually copied from another broker. Found:\n" + builder.toString());
            }

            RawMetaProperties rawProps = new RawMetaProperties(new ArrayList<>(brokerMetadataMap.values()).get(0));
            return new Pair<>(rawProps, offlineDirs);
        }
    }
}
