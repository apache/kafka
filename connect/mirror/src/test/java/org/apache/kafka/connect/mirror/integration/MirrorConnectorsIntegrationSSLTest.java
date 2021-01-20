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
package org.apache.kafka.connect.mirror.integration;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.test.TestUtils;
import kafka.server.KafkaConfig$;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

/**
 * Tests MM2 replication with SSL enabled at backup kafka cluster
 */
@Tag("integration")
public class MirrorConnectorsIntegrationSSLTest extends MirrorConnectorsIntegrationBaseTest {

    @BeforeEach
    public void startClusters() throws Exception {
        Map<String, Object> sslConfig = TestSslUtils.createSslConfig(false, true, Mode.SERVER, TestUtils.tempFile(), "testCert");
        // enable SSL on backup kafka broker
        backupBrokerProps.put(KafkaConfig$.MODULE$.ListenersProp(), "SSL://localhost:0");
        backupBrokerProps.put(KafkaConfig$.MODULE$.InterBrokerListenerNameProp(), "SSL");
        backupBrokerProps.putAll(sslConfig);
        
        Properties sslProps = new Properties();
        sslProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        sslProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, ((Password) sslConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).value());
        sslProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        
        // set SSL config for kafka connect worker
        backupWorkerProps.putAll(sslProps.entrySet().stream().collect(Collectors.toMap(
            e -> String.valueOf(e.getKey()), e ->  String.valueOf(e.getValue()))));
        
        mm2Props.putAll(sslProps.entrySet().stream().collect(Collectors.toMap(
            e -> BACKUP_CLUSTER_ALIAS + "." + String.valueOf(e.getKey()), e ->  String.valueOf(e.getValue()))));
        // set SSL config for producer used by source task in MM2
        mm2Props.putAll(sslProps.entrySet().stream().collect(Collectors.toMap(
            e -> BACKUP_CLUSTER_ALIAS + ".producer." + String.valueOf(e.getKey()), e ->  String.valueOf(e.getValue()))));
        
        super.startClusters();
    }
}

