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
package org.apache.kafka.testkit;

import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import scala.Some;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MiniKafkaNode implements AutoCloseable {
    private final String name;
    private final Map<String, String> fixedConfigs;
    private final LogContext logContext;
    private final Logger log;
    private final Collection<MiniKafkaLogDirBuilder> logDirBlds;
    private File rootDir;
    private Collection<MiniKafkaLogDir> logDirs = null;
    private KafkaServer kafkaServer = null;

    MiniKafkaNode(MiniKafkaClusterBuilder clusterBld, MiniKafkaNodeBuilder nodeBld,
                  String name, String zkString) {
        this.name = name;
        Map<String, String> config = new HashMap<>();
        setupNodeId(nodeBld, config);
        config.put("zookeeper.connect", zkString); //KafkaConfig$.MODULE$.ZkConnectProp
        this.logContext = new LogContext(clusterBld.logContext.logPrefix() + ": " + name + ": ");
        this.log = logContext.logger(MiniKafkaNode.class);
        if (nodeBld.logDirBlds.isEmpty()) {
            this.logDirBlds = new ArrayList<MiniKafkaLogDirBuilder>(1);
            this.logDirBlds.add(new MiniKafkaLogDirBuilder());
        } else {
            this.logDirBlds = new ArrayList<>(nodeBld.logDirBlds);
        }
        this.fixedConfigs = Collections.unmodifiableMap(
            TestKitUtil.mergeConfigs(clusterBld.configs, nodeBld.configs, config));
    }

    private static void setupNodeId(MiniKafkaNodeBuilder nodeBld, Map<String, String> config) {
        if (nodeBld.id == MiniKafkaNodeBuilder.INVALID_NODE_ID) {
            config.put("broker.id.generation.enable", "true"); //KafkaConfig$.MODULE$.BrokerIdGenerationEnableProp
        } else {
            config.put("broker.id.generation.enable", "false"); //KafkaConfig$.MODULE$.BrokerIdGenerationEnableProp
            config.put("broker.id", String.format("%d", nodeBld.id)); //KafkaConfig$.MODULE$.BrokerIdProp
        }
    }

    public void start() {
        close();
        boolean success = false;
        try {
            HashMap<String, String> effectiveConfigs = new HashMap<>(fixedConfigs);
            this.rootDir = TestKitUtil.createTempDir(name + "-");
            int logDirIdx = 0;
            ArrayList<String> logPaths = new ArrayList<>();
            this.logDirs = new ArrayList<>(logDirBlds.size());
            for (MiniKafkaLogDirBuilder logDirBld : logDirBlds) {
                MiniKafkaLogDir logDir = logDirBld.build(log,
                    new File(rootDir, String.format("oplog%d", logDirIdx++)));
                logDirs.add(logDir);
                logPaths.add(logDir.dir.getAbsolutePath());
            }
            effectiveConfigs.put("log.dirs", Utils.join(logPaths, ",")); // KafkaConfig.LogDirsProp
            effectiveConfigs.put("listeners", "PLAINTEXT://localhost:0"); // KakfaConfig.ListenersProp
            KafkaConfig config = new KafkaConfig(effectiveConfigs, false);
            this.kafkaServer = new KafkaServer(config, Time.SYSTEM, new Some<>(name),
                scala.collection.JavaConversions.<KafkaMetricsReporter>asScalaBuffer(
                    new ArrayList<KafkaMetricsReporter>()).seq());
            this.kafkaServer.startup();
            success = true;
        } finally {
            if (!success) {
                close();
            }
        }
    }

    public void shutdown() {
        if (this.kafkaServer != null) {
            try {
                this.kafkaServer.shutdown();
            } catch (Throwable e) {
                log.error("{}: Unable to shutdown KafkaServer", name, e);
            }
        }
    }

    @Override
    public void close() {
        shutdown();
        if (this.kafkaServer != null) {
            try {
                this.kafkaServer.awaitShutdown();
            } catch (Throwable e) {
                log.error("{} awaitShutdown error", name, e);
            } finally {
                this.kafkaServer = null;
            }
        }
        if (this.logDirs != null) {
            for (MiniKafkaLogDir logDir : this.logDirs) {
                logDir.close();
            }
            this.logDirs = null;
        }
        if (this.rootDir != null) {
            try {
                Utils.delete(rootDir);
            } catch (Throwable e) {
                log.error("{} error deleting {}", name, rootDir.getAbsolutePath());
            } finally {
                this.rootDir = null;
            }
        }
    }

    /**
     * Get the current value of a configuration key from the server.
     *
     * @param key   The configuration key.
     *
     * @returns     null if the server is not running;
     *              null if no such configuration key was found in the server's config;
     *              The associated configuration value otherwise.
     */
    public String config(String key) {
        if (kafkaServer == null) {
            log.trace("Unable to retrieve configuration {}: server is not running.", key);
            return null;
        }
        Object val;
        try {
            val = kafkaServer.config().get(key);
        } catch (ConfigException e) {
            log.trace("Unable to retrieve configuration {}: {}", key, e.getMessage());
            return null;
        }
        return val.toString();
    }

    /**
     * Returns the broker ID, or null if the broker is not running.
     */
    public Integer id() {
        return kafkaServer == null ? null : kafkaServer.config().brokerId();
    }
}
