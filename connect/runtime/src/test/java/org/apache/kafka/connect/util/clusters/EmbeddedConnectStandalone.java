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
package org.apache.kafka.connect.util.clusters;

import org.apache.kafka.connect.cli.ConnectStandalone;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.PLUGIN_DISCOVERY_CONFIG;
import static org.apache.kafka.connect.runtime.rest.RestServerConfig.LISTENERS_CONFIG;
import static org.apache.kafka.connect.runtime.standalone.StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG;

/**
 * Start a standalone embedded connect worker. Internally, this class will spin up a Kafka and Zk cluster,
 * set up any tmp directories. and clean them up on exit. Methods on the same
 * {@code EmbeddedConnectStandalone} are not guaranteed to be thread-safe.
 */
public class EmbeddedConnectStandalone extends EmbeddedConnect {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedConnectStandalone.class);

    private static final String REST_HOST_NAME = "localhost";

    private final Map<String, String> workerProps;
    private final String offsetsFile;

    private WorkerHandle connectWorker;

    private EmbeddedConnectStandalone(
            int numBrokers,
            Properties brokerProps,
            boolean maskExitProcedures,
            Map<String, String> clientProps,
            Map<String, String> workerProps,
            String offsetsFile
    ) {
        super(numBrokers, brokerProps, maskExitProcedures, clientProps);
        this.workerProps = workerProps;
        this.offsetsFile = offsetsFile;
    }

    @Override
    public void startConnect() {
        log.info("Starting standalone Connect worker");

        workerProps.put(BOOTSTRAP_SERVERS_CONFIG, kafka().bootstrapServers());
        // use a random available port
        workerProps.put(LISTENERS_CONFIG, "HTTP://" + REST_HOST_NAME + ":0");

        workerProps.putIfAbsent(OFFSET_STORAGE_FILE_FILENAME_CONFIG, offsetsFile);
        workerProps.putIfAbsent(KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        workerProps.putIfAbsent(VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        workerProps.putIfAbsent(PLUGIN_DISCOVERY_CONFIG, "hybrid_fail");

        Connect connect = new ConnectStandalone().startConnect(workerProps);
        connectWorker = new WorkerHandle("standalone", connect);
    }

    @Override
    public String toString() {
        return String.format("EmbeddedConnectStandalone(numBrokers= %d, workerProps= %s)",
            numBrokers,
            workerProps);
    }

    @Override
    protected Set<WorkerHandle> workers() {
        return connectWorker != null
                ? Collections.singleton(connectWorker)
                : Collections.emptySet();
    }

    public static class Builder extends EmbeddedConnectBuilder<EmbeddedConnectStandalone, Builder> {

        private String offsetsFile = null;

        public Builder offsetsFile(String offsetsFile) {
            this.offsetsFile = offsetsFile;
            return this;
        }

        @Override
        protected EmbeddedConnectStandalone build(
                int numBrokers,
                Properties brokerProps,
                boolean maskExitProcedures,
                Map<String, String> clientProps,
                Map<String, String> workerProps
        ) {
            if (offsetsFile == null)
                offsetsFile = tempOffsetsFile();

            return new EmbeddedConnectStandalone(
                    numBrokers,
                    brokerProps,
                    maskExitProcedures,
                    clientProps,
                    workerProps,
                    offsetsFile
            );
        }

        private String tempOffsetsFile() {
            try {
                return TestUtils
                        .tempFile("connect-standalone-offsets", null)
                        .getAbsolutePath();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to create temporary offsets file", e);
            }
        }
    }

}
