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
package org.apache.kafka.streams.processor.internals.testutil;

import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode;

import java.util.Properties;

public class DummyStreamsConfig extends StreamsConfig {

    public DummyStreamsConfig() {
        super(dummyProps(ProcessingMode.AT_LEAST_ONCE, false));
    }

    public DummyStreamsConfig(final ProcessingMode processingMode) {
        super(dummyProps(processingMode, false));
    }

    public DummyStreamsConfig(final ProcessingMode processingMode, final boolean streamsProtocolEnabled) {
        super(dummyProps(processingMode, streamsProtocolEnabled));
    }

    private static Properties dummyProps(final ProcessingMode processingMode, final boolean streamsProtocolEnabled) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dummy-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingMode.toString());
        if (streamsProtocolEnabled) {
            props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name);
        }
        return props;
    }
}
