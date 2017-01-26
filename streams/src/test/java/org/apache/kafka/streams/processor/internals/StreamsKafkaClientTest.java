/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;

import java.util.Properties;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StreamsKafkaClientTest {

    @Test
    public void testConfigFromStreamsConfig() {
        for (final String expectedMechanism : asList("PLAIN", "SCRAM-SHA-512")) {
            final Properties props = new Properties();
            props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "some_app_id");
            props.setProperty(SaslConfigs.SASL_MECHANISM, expectedMechanism);
            props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
            final StreamsConfig streamsConfig = new StreamsConfig(props);
            final AbstractConfig config = StreamsKafkaClient.Config.fromStreamsConfig(streamsConfig);
            assertEquals(expectedMechanism, config.values().get(SaslConfigs.SASL_MECHANISM));
            assertEquals(expectedMechanism, config.getString(SaslConfigs.SASL_MECHANISM));
        }
    }

}
